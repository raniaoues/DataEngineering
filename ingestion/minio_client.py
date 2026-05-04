from minio import Minio 
from minio.error import S3Error
import os 
import io
from dotenv import load_dotenv

load_dotenv()

def get_minio_client():
    """
     fonction nasen3ou beha MinIO client 
     naaytoulha kol ma bsh nesthakou n interactiw maa storage  
    """
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),#address mtee l minio server mteena 
        access_key=os.getenv("MINIO_ACCESS_KEY"),#username lel authentification 
        secret_key=os.getenv("MINIO_SECRET_KEY"),#password lel authentification 
        secure=False
        # secure=True would require HTTPS (SSL certificate)
        # We're running locally so HTTP is fine
        # In production on AWS/GCP, always use secure=True
    )
    return client

def ensure_bucket_exits(client,bucket_name):
    """
    Creates the bucket if it doesn't already exist.

    WHY this function?
    If you try to upload a file to a bucket that doesn't exist,
    MinIO throws an error and your data is lost.
    This function makes sure the bucket always exists BEFORE uploading.
    Think of it like: mkdir -p (create folder if not exists)
    """

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' created")
    else:
        print(f"📦 Bucket '{bucket_name}' already exists")

def upload_dataframe(df, object_name):
    """
    Saves a Pandas DataFrame as a CSV file directly into MinIO.

    Parameters:
        df          → the DataFrame to save (our fire data)
        object_name → the filename inside MinIO (like a path)
                      example: 'viirs/2024/01/15/fires_14h00.csv'
    """
    bucket_name= os.getenv("MINIO_BUCKET")
    client= get_minio_client()
    ensure_bucket_exits(client,bucket_name)

    csv_buffer=io.BytesIO()
     # WHY BytesIO?
    # Normally pd.to_csv() saves to a file on disk.
    # BytesIO creates a "virtual file" in RAM — no disk needed.
    # MinIO's upload function expects a file-like object, so this works perfectly.

    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0) #bsh nerjouu l cursor l awel buffer khter mbaad makteb kaad fil lekhr donc ken nbaathouh akeka bsh iji file fergh
    file_size=csv_buffer.getbuffer().nbytes
    # WHY get file size?
    # MinIO's put_object() REQUIRES knowing the file size in advance
    # to properly handle the upload stream

    client.put_object(
        bucket_name,
        object_name,
        csv_buffer,
        file_size,
        content_type="text/csv"
    )
    print(f" Uploaded '{object_name}' to bucket '{bucket_name}' "
          f"({file_size} bytes)")
