from minio import Minio
from minio.error import S3Error
import os
import io
import streamlit as st

def get_minio_client():
    # Sur Streamlit Cloud → utilise st.secrets
    # En local → utilise les variables d'environnement comme fallback
    try:
        endpoint   = st.secrets["MINIO_ENDPOINT"]
        access_key = st.secrets["MINIO_ACCESS_KEY"]
        secret_key = st.secrets["MINIO_SECRET_KEY"]
        secure     = st.secrets.get("MINIO_SECURE", False)
    except Exception:
        endpoint   = os.getenv("MINIO_ENDPOINT")
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")
        secure     = False

    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
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
