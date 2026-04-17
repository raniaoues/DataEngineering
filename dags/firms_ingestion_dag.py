from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import sys

# ✅ FIX 1: Wrong path separators
# ❌ Wrong:  '\opt\airflow\ingestion'  (backslashes = Windows style)
# ✅ Correct: '/opt/airflow/ingestion' (forward slashes = Linux style)
# WHY? Airflow runs inside a Linux Docker container, not Windows.
# Backslashes are interpreted as escape characters in Python strings,
# so '\opt' becomes a weird character, not a folder path.
sys.path.insert(0, '/opt/airflow/ingestion')

default_args = {
    'owner': 'fire_pipeline',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,  # ✅ FIX 2: Disabled until SMTP is configured
    'email_on_retry': False,    # ✅ Prevents ConnectionRefusedError spam
    # WHY disable for now?
    # Airflow tries to send emails via SMTP but no SMTP server
    # is configured yet — causes a secondary error that hides
    # the real error message. We fix SMTP separately later.
}

with DAG(
    dag_id='firms_ingestion_dag',
    default_args=default_args,
    description='Hourly ingestion of NASA FIRMS fire data',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ingestion', 'nasa', 'firms'],
) as dag:

    # -------------------------------------------------------
    # TASK 1: Fetch fire data from NASA FIRMS
    # -------------------------------------------------------
    def fetch_firms_task():
        from nasa_firms_client import fetch_fire_data
        
        df = fetch_fire_data()

        if df.empty:
            raise ValueError("❌ NASA returned empty data — aborting")

        print(f"✅ Fetched {len(df)} fire hotspots")
        # ✅ FIX: Serialize to JSON so tasks 2 & 3 can reuse the SAME data
        # WHY? Without this, each task calls NASA independently — 3 API calls
        # instead of 1, and each fetch could return slightly different data
        # (fires update in real time). Validate and Upload must work on the
        # exact same snapshot that was fetched.
        return df.to_json()

    fetch_task = PythonOperator(
        task_id='fetch_firms_viirs',
        python_callable=fetch_firms_task,
    )

    # -------------------------------------------------------
    # TASK 2: Validate the data quality
    # -------------------------------------------------------
    def validate_data_task(**context):
        import pandas as pd
        # ✅ FIX: Pull the data fetched by task 1 via XCom — no re-fetch
        ti = context['ti']
        df_json = ti.xcom_pull(task_ids='fetch_firms_viirs')
        if df_json is None:
            raise ValueError("❌ No data received from fetch task.")
        df = pd.read_json(df_json)

        # Check 1: Required columns exist
        required_columns = ['latitude', 'longitude', 'frp', 'confidence']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"❌ Missing columns: {missing}")

        # Check 2: No completely null rows
        null_rows = df[required_columns].isnull().all(axis=1).sum()
        if null_rows > len(df) * 0.5:
            raise ValueError(f"❌ More than 50% null rows: {null_rows}")

        # Check 3: Coordinates are valid
        invalid_coords = df[
            (df['latitude'] < -90)  | (df['latitude'] > 90) |
            (df['longitude'] < -180) | (df['longitude'] > 180)
        ]
        if len(invalid_coords) > 0:
            print(f"⚠️ Warning: {len(invalid_coords)} invalid coordinates found")

        print("✅ Data validation passed")

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task,
        provide_context=True,
    )

    def clean_data_task():
        from nasa_firms_client import fetch_fire_data
        import pandas as pd

        df = fetch_fire_data()

        before = len(df)
        
        # 1. Drop duplicates
        df = df.drop_duplicates(subset=['latitude', 'longitude', 'acq_date', 'acq_time'])
        
        # 2. Drop rows missing critical fields
        df = df.dropna(subset=['latitude', 'longitude', 'frp'])
        
        # 3. Remove invalid FRP
        df = df[df['frp'] >= 0]
        
        # 4. Normalize confidence to numeric
        confidence_map = {'l': 0, 'n': 1, 'h': 2}
        if df['confidence'].dtype == object:
            df['confidence'] = df['confidence'].map(confidence_map)
        
        # 5. Cast types
        df['latitude'] = df['latitude'].astype(float)
        df['longitude'] = df['longitude'].astype(float)
        df['frp'] = df['frp'].astype(float)

        after = len(df)
        print(f"✅ Cleaning done: {before} → {after} rows ({before - after} removed)")

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data_task,
    )

    # -------------------------------------------------------
    # TASK 4: Upload validated data to MinIO
    # -------------------------------------------------------
    def upload_minio_task(**context):
        import pandas as pd
        from minio_client import upload_dataframe
        from datetime import datetime

        # ✅ FIX: Pull the same data from XCom — no re-fetch
        ti = context['ti']
        df_json = ti.xcom_pull(task_ids='fetch_firms_viirs')
        if df_json is None:
            raise ValueError("❌ No data received from fetch task.")
        df = pd.read_json(df_json)

        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        object_name = f"viirs/{timestamp}.csv"
        upload_dataframe(df, object_name)
        print(f"✅ Saved to MinIO: {object_name}")

    upload_task = PythonOperator(
        task_id='upload_minio',
        python_callable=upload_minio_task,
        provide_context=True,
    )

    # Task dependencies — order of execution
    fetch_task >> validate_task >> clean_task >> upload_task