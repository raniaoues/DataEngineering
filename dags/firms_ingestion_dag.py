from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta 
import sys

sys.path.insert(0,'/opt/airflow/ingestion') #ami airflow y runni fil container envirnment mteou donc maayrech win les scripts mteena mahtoutin heka aleh lezm ahana naatiwh l path manuallement 


#ejeyin houma settings general ala les tasks lkol fi west e dag hedhi 

default_args ={
    'owner':'fire_pipline' #l responsable ala e dag 
    ,
    'retries':3 #saat l api nasa ywali maash available donc ami overflow y jareb 3 fois kbal mayhotha as failure 
    ,
    'retry_delay': timedelta(minutes=10)#bin e retry w retry yestna 10
    ,
    'email_on_failure': True #ybaath email if oit failed
    ,
    'email': ['Khadija.elloumi03@gmail.com']
}

# DAG DEFINITION 

with DAG (
    dag_id='firms_ingestion_dag' #unique name lel dag bsh yodher fil airflow UI
    ,
    default_args=default_args
    ,
    description='Hourly ingestion of NASA FIRMS fire data '
    ,
    sechedule_interval='0 * * * *' # cron expression: at minute 0 of every hour = every hour
    ,
    start_date=datetime(2024, 1, 1)
    ,
    catchup=False #only run from NOW onwards, ignore the past
    ,
    tags=['ingestion', 'nasa', 'firms']

) as dag :
    
    # TASK 1: Fetch fire data from NASA FIRMS

    def fetch_firms_task():
        """
            l function hedhi taamel call l nasa api w trouterni fire data
            naamelou function naaytou fiha lel fonction ili khdmneha kbal khter airflow's PythonOperator requires a callable (function)
            donc  naamlou definition lel logic hnee w baad naadiwha lel operator
        
        """
        from ingestion.nasa_firms_client import fetch_fire_data
        
        df=fetch_fire_data()
        if df.empty:
            raise ValueError(" Nasa returned empty file ") #lezm n raisiw error bsh airflow yaref ili fama failure w yaamel retry 
        
        print(f" Fetched {len(df)} fire hotspots")
        return len(df)
    
    fetch_task = PythonOperator(
        task_id='fetch_firms_viirs'#id unique le task
        ,python_callable=fetch_firms_task,
    )

    # TASK 2: Validate the data quality

    def validate_data_task():
        """
            Checks that the fetched data meets minimum quality standards
            Bad data flowing through the pipeline is worse than no data.
            Catching issues early prevents corrupted reports downstream.
        
        """
        from ingestion.nasa_firms_client import fetch_fire_data
        df = fetch_fire_data()

        # Check 1: Required columns exist
        required_columns = ['latitude', 'longitude', 'frp', 'confidence']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        # Check 2: No completely null rows
        null_rows = df[required_columns].isnull().all(axis=1).sum()
        if null_rows > len(df) * 0.5:
            raise ValueError(f"More than 50% null rows: {null_rows}")
        
        # Check 3: Coordinates are valid
        invalid_coords = df[
            (df['latitude'] < -90) | (df['latitude'] > 90) |
            (df['longitude'] < -180) | (df['longitude'] > 180)
        ]
        if len(invalid_coords) > 0:
            print(f"Warning: {len(invalid_coords)} invalid coordinates found")

        
        print(f"Data validation passed")
    
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_task,
    )


    # TASK 3: Upload validated data to MinIO

    def upload_minio_task():
        """
        n saviw e data as csv file fi MinIO 
        """
        from ingestion.nasa_firms_client import fetch_fire_data
        from ingestion.minio_client import upload_dataframe
        from datetime import datetime

        df = fetch_fire_data()
        timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H%M%S")
        object_name = f"viirs/{timestamp}.csv"
        upload_dataframe(df, object_name)
        print(f"Saved to MinIO: {object_name}")

    upload_task = PythonOperator(
        task_id='upload_minio',
        python_callable=upload_minio_task,
    )

    fetch_task >> validate_task >> upload_task






    