from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/ingestion')

default_args = {
    'owner': 'fire_pipeline',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False,
}

@provide_session
def get_latest_ingestion_run_dt(dt, session=None):
    last_run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == 'firms_ingestion_dag',
            DagRun.state == 'success',
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    if last_run is None:
        raise ValueError("❌ No successful run of firms_ingestion_dag found.")
    return last_run.execution_date


with DAG(
    dag_id='enrichment_dag',
    default_args=default_args,
    description='Daily enrichment: weather + forest + country data',
    schedule_interval='0 8 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['enrichment', 'weather', 'forest'],
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='firms_ingestion_dag',
        external_task_id='upload_minio',
        execution_date_fn=get_latest_ingestion_run_dt,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=3600,
        poke_interval=60,
        mode='reschedule',
    )

    # -------------------------------------------------------
    # TASK 1: Fetch weather data from Open-Meteo
    # -------------------------------------------------------
    def fetch_meteo_task():
        import requests
        import pandas as pd
        from minio_client import get_minio_client
        import io
        import os
        from datetime import datetime

        client = get_minio_client()
        bucket = os.getenv("MINIO_BUCKET")

        today = datetime.utcnow().strftime("%Y/%m/%d")
        objects = client.list_objects(bucket, prefix=f"viirs/{today}/")

        all_dfs = []
        for obj in objects:
            response = client.get_object(bucket, obj.object_name)
            df = pd.read_csv(io.BytesIO(response.read()))
            all_dfs.append(df)

        if not all_dfs:
            raise ValueError("❌ No fire data found for today")

        fires_df = pd.concat(all_dfs, ignore_index=True)

        enriched_rows = []
        for _, row in fires_df.head(100).iterrows():
            meteo_url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={row['latitude']}"
                f"&longitude={row['longitude']}"
                f"&current_weather=true"
                f"&hourly=relativehumidity_2m"
            )
            try:
                resp = requests.get(meteo_url, timeout=5)
                weather = resp.json().get('current_weather', {})
                row = row.copy()
                row['temperature_c'] = weather.get('temperature')
                row['windspeed_kmh'] = weather.get('windspeed')
                row['humidity_pct'] = (
                    resp.json()
                    .get('hourly', {})
                    .get('relativehumidity_2m', [None])[0]
                )
            except Exception as e:
                print(f"⚠️ Weather fetch failed for row: {e}")
                row = row.copy()
                row['temperature_c'] = None
                row['windspeed_kmh'] = None
                row['humidity_pct'] = None

            enriched_rows.append(row)

        enriched_df = pd.DataFrame(enriched_rows)
        print(f"✅ Weather enrichment done: {len(enriched_df)} rows")
        return enriched_df.to_json()

    fetch_meteo = PythonOperator(
        task_id='fetch_meteo',
        python_callable=fetch_meteo_task,
    )

    # -------------------------------------------------------
    # TASK 2: Fetch forest coverage from GFW
    # -------------------------------------------------------
    def fetch_gfw_task(**context):
        import pandas as pd
        from gfw_client import get_forest_coverage

        ti = context['ti']
        enriched_json = ti.xcom_pull(task_ids='fetch_meteo')

        if enriched_json is None:
            raise ValueError("❌ No data received from fetch_meteo task.")

        enriched_df = pd.read_json(enriched_json)

        enriched_df['forest_pct'] = None
        enriched_df['primary_forest'] = None
        enriched_df['protected_area'] = None

        for idx, row in enriched_df.iterrows():
            try:
                forest_data = get_forest_coverage(
                    lat=row['latitude'],
                    lon=row['longitude'],
                    radius_km=5.0
                )
                enriched_df.at[idx, 'forest_pct'] = forest_data['forest_pct']
                enriched_df.at[idx, 'primary_forest'] = forest_data['primary_forest']
                enriched_df.at[idx, 'protected_area'] = forest_data['protected_area']
            except Exception as e:
                print(f"⚠️ GFW enrichment failed for row {idx}: {e}")

        print(f"✅ GFW enrichment complete: {len(enriched_df)} rows processed")
        return enriched_df.to_json()

    fetch_gfw = PythonOperator(
        task_id='fetch_gfw',
        python_callable=fetch_gfw_task,
        provide_context=True,
    )

    # -------------------------------------------------------
    # TASK 3: Reverse geocoding — add 'country' column
    # -------------------------------------------------------
    def add_country_task(**context):
        """
        Converts (latitude, longitude) → country name using
        a local shapefile from Natural Earth — no API calls,
        no rate limits, fully offline and instant.

        WHY offline shapefile and not an API like Nominatim?
        - Nominatim allows ~1 req/second → too slow for thousands of fires
        - No API key, no cost, no network dependency
        - geopandas sjoin processes all rows in one vectorized operation

        WHY round coordinates to 1 decimal?
        - Two fires at (48.12, 17.34) and (48.19, 17.31) are both in Slovakia
        - Rounding reduces unique points → faster spatial join
        - 0.1° ≈ 11km resolution — more than enough for country attribution

        Pipeline becomes:
        fetch → weather → forest → ✅ country → save
        """
        import pandas as pd
        import geopandas as gpd
        from shapely.geometry import Point
        import requests
        import os
        import zipfile
        import io as io_module

        ti = context['ti']
        enriched_json = ti.xcom_pull(task_ids='fetch_gfw')

        if enriched_json is None:
            raise ValueError("❌ No data received from fetch_gfw task.")

        enriched_df = pd.read_json(enriched_json)

        # Step 1: Download Natural Earth shapefile if not already present
        # WHY Natural Earth?
        # Public domain standard dataset for country boundaries.
        # 110m resolution is perfect for country-level attribution.
        shapefile_dir = '/tmp/ne_110m_admin_0_countries'
        shapefile_path = f'{shapefile_dir}/ne_110m_admin_0_countries.shp'

        if not os.path.exists(shapefile_path):
            print("📥 Downloading Natural Earth country shapefile...")
            url = (
                "https://naturalearth.s3.amazonaws.com/110m_cultural/"
                "ne_110m_admin_0_countries.zip"
            )
            resp = requests.get(url, timeout=30)
            with zipfile.ZipFile(io_module.BytesIO(resp.content)) as z:
                z.extractall(shapefile_dir)
            print("✅ Shapefile downloaded and extracted")

        # Step 2: Load world country polygons
        world = gpd.read_file(shapefile_path)
        # Each row = one country polygon, 'NAME' = English country name

        # Step 3: Round coordinates to reduce unique points
        # WHY round to 1 decimal?
        # 5000 fires → maybe 400 unique 0.1° cells → much faster join
        enriched_df['lat_r'] = enriched_df['latitude'].round(1)
        enriched_df['lon_r'] = enriched_df['longitude'].round(1)

        unique_coords = enriched_df[['lat_r', 'lon_r']].drop_duplicates()

        unique_gdf = gpd.GeoDataFrame(
            unique_coords,
            geometry=unique_coords.apply(
                lambda row: Point(row['lon_r'], row['lat_r']), axis=1
                # WHY Point(lon, lat)?
                # Shapely uses (x=longitude, y=latitude) — geographic standard
            ),
            crs='EPSG:4326'
            # WHY EPSG:4326?
            # WGS84 — the coordinate system used by GPS, NASA FIRMS,
            # and all geographic APIs. Must match the shapefile CRS.
        )

        # Step 4: Spatial join — which country polygon contains each point?
        joined = gpd.sjoin(
            unique_gdf,
            world[['NAME', 'geometry']],
            how='left',
            predicate='within'
            # WHY 'within'?
            # Points that fall inside a country polygon get that country's name.
            # Points in oceans or on borders return NaN → handled below.
        )

        # Step 5: Build lookup dict and map back to the full dataframe
        coord_to_country = dict(
            zip(
                zip(joined['lat_r'], joined['lon_r']),
                joined['NAME']
            )
        )

        enriched_df['country'] = enriched_df.apply(
            lambda row: coord_to_country.get(
                (row['lat_r'], row['lon_r']), 'Unknown'
                # WHY 'Unknown' fallback?
                # Points in international waters, disputed zones, or exactly
                # on a border won't match any polygon. 'Unknown' keeps the
                # row instead of dropping it — important for data completeness.
            ),
            axis=1
        )

        # Step 6: Clean up helper columns
        enriched_df = enriched_df.drop(columns=['lat_r', 'lon_r'])

        country_counts = enriched_df['country'].value_counts()
        print(f"✅ Country enrichment done: {len(enriched_df)} rows")
        print(f"   Top countries: {country_counts.head(5).to_dict()}")
        print(f"   Unknown (ocean/border): {(enriched_df['country'] == 'Unknown').sum()} rows")

        return enriched_df.to_json()

    add_country = PythonOperator(
        task_id='add_country',
        python_callable=add_country_task,
        provide_context=True,
    )

    # -------------------------------------------------------
    # TASK 4: Save final enriched data to MinIO
    # -------------------------------------------------------
    def save_enriched_task(**context):
        import pandas as pd
        from minio_client import upload_dataframe
        from datetime import datetime

        ti = context['ti']
        # ✅ Pull from add_country (was fetch_gfw before adding geocoding step)
        enriched_json = ti.xcom_pull(task_ids='add_country')

        if enriched_json is None:
            raise ValueError("❌ No data received from add_country task.")

        enriched_df = pd.read_json(enriched_json)

        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        object_name = f"enriched/{timestamp}.csv"
        upload_dataframe(enriched_df, object_name)
        print(f"✅ Enriched data saved: {object_name}")
        print(f"   Columns: {list(enriched_df.columns)}")

    save_enriched = PythonOperator(
        task_id='save_enriched',
        python_callable=save_enriched_task,
        provide_context=True,
    )

    # ✅ Updated pipeline — add_country inserted between fetch_gfw and save_enriched
    wait_for_ingestion >> fetch_meteo >> fetch_gfw >> add_country >> save_enriched