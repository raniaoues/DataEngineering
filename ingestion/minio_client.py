from supabase import create_client
import os
import io
import streamlit as st

def get_supabase_client():
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except Exception:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)

def ensure_bucket_exits(client, bucket_name):
    # Supabase crée le bucket via le dashboard, pas besoin de le créer en code
    print(f"📦 Using bucket '{bucket_name}'")

def upload_dataframe(df, object_name):
    try:
        bucket_name = st.secrets["MINIO_BUCKET"]
    except Exception:
        bucket_name = os.getenv("MINIO_BUCKET", "fires-raw")

    client = get_supabase_client()

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    file_bytes = csv_buffer.getvalue()

    client.storage.from_(bucket_name).upload(
        path=object_name,
        file=file_bytes,
        file_options={"content-type": "text/csv", "upsert": "true"}
    )
    print(f"✅ Uploaded '{object_name}' to bucket '{bucket_name}'")

def download_dataframe(object_name):
    """Remplace la lecture depuis MinIO"""
    try:
        bucket_name = st.secrets["MINIO_BUCKET"]
    except Exception:
        bucket_name = os.getenv("MINIO_BUCKET", "fires-raw")

    client = get_supabase_client()
    response = client.storage.from_(bucket_name).download(object_name)
    
    import pandas as pd
    return pd.read_csv(io.BytesIO(response))