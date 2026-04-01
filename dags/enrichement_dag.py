from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/ingestion')
default_args = {
    'owner': 'fire_pipeline',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,
    'email': ['khadija.elloumi03@gmail.com'],
}

with DAG(
    dag_id='enrichment_dag',
    default_args=default_args,
    description='Daily enrichment: weather + forest data',
    schedule_interval='0 8 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['enrichment', 'weather', 'forest'],
)as dag:
    
    # SENSOR: Wait for ingestion DAG to finish first

    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='firms_ingestion_dag',
        # The DAG we're waiting for

        external_task_id='upload_minio',
        timeout=3600,
        # If ingestion DAG hasn't finished after 1 hour, something is wrong.
        # We stop waiting and mark this task as failed
        # rather than waiting forever.

        mode='poke',
        # 'poke': checks every 30 seconds if the external task is done
        # 'reschedule': releases the worker slot between checks 
        # We use poke because ingestion should finish quickly

    )