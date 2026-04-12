from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.models import DagRun
from datetime import datetime, timedelta
import sys 

sys.path.insert(0, '/opt/airflow/ingestion')

# ✅ FIX: Dynamically find the latest successful run of enrichment_dag
# WHY? execution_delta assumes both DAGs run on a fixed schedule offset.
# For manual triggers or mismatched schedules, the delta will never match —
# the sensor loops forever poking for a run that doesn't exist.
# This function queries the DB for the actual latest successful run instead.
@provide_session
def get_latest_enrichment_run_dt(dt, session=None):
    last_run = (
        session.query(DagRun)
        .filter(
            DagRun.dag_id == 'enrichment_dag',
            DagRun.state == 'success',
        )
        .order_by(DagRun.execution_date.desc())
        .first()
    )
    if last_run is None:
        raise ValueError("❌ No successful run of enrichment_dag found.")
    return last_run.execution_date


default_args = {
    'owner': 'fire_pipeline',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,  # ✅ Disabled — no SMTP configured yet
    'email_on_retry': False,
}

with DAG(
    dag_id='reporting_dag',
    default_args=default_args,
    description='Daily aggregation, trend detection and alerts',
    schedule_interval='0 9 * * *',
    # Runs at 9AM — one hour AFTER enrichment_dag (which runs at 8AM)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['reporting', 'alerts', 'trends'],
) as dag:

    # Wait for enrichment to finish before aggregating
    wait_for_enrichment = ExternalTaskSensor(
        task_id='wait_for_enrichment',
        external_dag_id='enrichment_dag',
        external_task_id='save_enriched',
        execution_date_fn=get_latest_enrichment_run_dt,  # ✅ FIX: replaces execution_delta
        # WHY execution_date_fn instead of execution_delta?
        # execution_delta looks for enrichment_dag at an exact time offset.
        # If that exact run doesn't exist (e.g. first run, manual trigger),
        # it polls forever. execution_date_fn finds the actual latest
        # successful run dynamically — always works.
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=7200,
        poke_interval=60,
        mode='reschedule',
    )

    # -------------------------------------------------------
    # TASK 1: Aggregate fire data with DuckDB
    # -------------------------------------------------------
    def aggregate_task():
        """
        WHY DuckDB here and not PostgreSQL?
        DuckDB can query CSV files directly from MinIO without
        loading them into a database first — extremely fast and simple.
        Perfect for aggregation before loading to PostgreSQL gold layer.
        """
        import duckdb
        import os
        from datetime import datetime
        today = datetime.utcnow().strftime("%Y/%m/%d")

        minio_endpoint = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")

        con = duckdb.connect()
        # Create an in-memory DuckDB connection

        con.execute(f"""
            SET s3_endpoint='{minio_endpoint}';
            SET s3_access_key_id='{access_key}';
            SET s3_secret_access_key='{secret_key}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        # DuckDB has a native S3 reader but needs credentials
        # to connect to MinIO just like any S3 client
        # Aggregate: count fires per country
        result = con.execute(f"""
            SELECT
                country,
                COUNT(*) as fire_count,
                AVG(frp) as avg_frp,
                MAX(frp) as max_frp,
                AVG(temperature_c) as avg_temp,
                AVG(humidity_pct) as avg_humidity
            FROM read_csv_auto('s3://fires-raw/enriched/{today}/*.csv')
            GROUP BY country
            ORDER BY fire_count DESC
        """).df()

        print(f"✅ Aggregation done: {len(result)} countries")
        print(result.head(10))
        return result.to_json()

    aggregate = PythonOperator(
        task_id='duckdb_aggregate',
        python_callable=aggregate_task,
    )

    # -------------------------------------------------------
    # TASK 2: Detect abnormal trends
    # -------------------------------------------------------
    def detect_trends_task(**context):
        """
        Compares current fire counts vs same period last year.
        Flags countries with >50% increase as alerts.
        WHY 50% threshold?
        Small variations are normal. 50% increase is statistically
        significant enough to indicate a genuine worsening situation.
        """
        import pandas as pd
        from sqlalchemy import create_engine
        from datetime import datetime

        ti = context['ti']
        current_json = ti.xcom_pull(task_ids='duckdb_aggregate')
        current_df = pd.read_json(current_json)

        # Step 1: Connect to PostgreSQL gold layer
        engine = create_engine(
            'postgresql://airflow:airflow@airflow-db:5432/airflow'
        )

        # Step 2: Fetch last year's data for the same month
        current_month = datetime.utcnow().month
        current_year = datetime.utcnow().year
        last_year = current_year - 1

        try:
            last_year_df = pd.read_sql(
                f"""
                SELECT
                    country,
                    SUM(fire_count) as fire_count_last_year
                FROM fire_country_daily
                WHERE
                    EXTRACT(YEAR  FROM report_date) = {last_year}
                    AND EXTRACT(MONTH FROM report_date) = {current_month}
                GROUP BY country
                """,
                engine
            )
        except Exception:
            # WHY catch exception here?
            # On the very first run, fire_country_daily doesn't exist yet.
            # We create an empty DataFrame so the pipeline can still complete
            # and create the table for next year's comparison.
            print("⚠️ No historical data yet — first run, skipping trend comparison")
            last_year_df = pd.DataFrame(columns=['country', 'fire_count_last_year'])

        # Step 3: Merge current vs last year on country
        merged_df = current_df.merge(
            last_year_df,
            on='country',
            how='left'
            # WHY left join?
            # Keep ALL countries from current data.
            # If no last year data, fire_count_last_year will be NULL.
        )

        merged_df['fire_count'] = pd.to_numeric(merged_df['fire_count'], errors='coerce')
        merged_df['fire_count_last_year'] = pd.to_numeric(merged_df['fire_count_last_year'], errors='coerce')

        merged_df['pct_change'] = (
            (merged_df['fire_count'] - merged_df['fire_count_last_year'])
            / merged_df['fire_count_last_year'].replace(0, float('nan'))  # ✅ NaN not None
            * 100
        ).round(1)

        # Step 4: Flag alerts
        merged_df['alert_triggered'] = merged_df['pct_change'] > 50
        merged_df['alert_triggered'] = merged_df['alert_triggered'].fillna(False)

        # Step 5: Add report_date for future year-over-year queries
        merged_df['report_date'] = datetime.utcnow().date()

        # Step 6: Log results clearly
        alerts = merged_df[merged_df['alert_triggered'] == True]
        no_data = merged_df[merged_df['pct_change'].isna()]

        print(f"\n📊 Trend Detection Summary:")
        print(f"  Total countries analyzed : {len(merged_df)}")
        print(f"  Countries with alerts    : {len(alerts)}")
        print(f"  Countries with no history: {len(no_data)}")

        if not alerts.empty:
            print(f"\n🚨 ALERTS TRIGGERED:")
            for _, row in alerts.iterrows():
                print(
                    f"  ⚠️  {row['country']}: "
                    f"{int(row['fire_count'])} fires this year vs "
                    f"{int(row['fire_count_last_year'])} last year "
                    f"(+{row['pct_change']}%)"
                )
        else:
            print("\n✅ No abnormal trends detected")

        return merged_df.to_json()

    detect_trends = PythonOperator(
        task_id='compute_trends',
        python_callable=detect_trends_task,
        provide_context=True,
    )

    # -------------------------------------------------------
    # TASK 3: Load results to PostgreSQL gold layer
    # -------------------------------------------------------
    def load_gold_task(**context):
        """
        Saves final aggregated and enriched data to PostgreSQL.
        WHY PostgreSQL at this stage and not earlier?
        PostgreSQL is for FINAL, clean, analysis-ready data.
        Raw and intermediate data stays in MinIO (cheap storage).
        Only gold-quality data goes to PostgreSQL (fast queries).
        This is the Lakehouse pattern: cheap storage → fast DB
        """
        import pandas as pd
        from sqlalchemy import create_engine

        ti = context['ti']
        final_json = ti.xcom_pull(task_ids='compute_trends')
        final_df = pd.read_json(final_json)

        engine = create_engine(
            'postgresql://airflow:airflow@airflow-db:5432/airflow'
        )

        final_df.to_sql(
            'fire_country_daily',
            engine,
            if_exists='replace',
            # WHY replace?
            # 'replace' drops and recreates the table each time.
            # For daily reports, replace ensures data is always fresh.
            index=False
        )
        print(f"✅ Gold layer loaded: {len(final_df)} rows → PostgreSQL")

    load_gold = PythonOperator(
        task_id='pg_load',
        python_callable=load_gold_task,
        provide_context=True,
    )

    wait_for_enrichment >> aggregate >> detect_trends >> load_gold