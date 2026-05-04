# migrate_minio_to_supabase.py
from minio import Minio
from supabase import create_client
import pandas as pd
import io

# ── Connexion MinIO local ──
minio_client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

# ── Connexion Supabase ──
SUPABASE_URL = "https://oufkevtlizjdawzwxncr.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im91ZmtldnRsaXpqZGF3end4bmNyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzgwODI4NywiZXhwIjoyMDkzMzg0Mjg3fQ.n2BT_Bv-pBkMKgdT0cUvINblFkdQfBybt60VNSnIoUs"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET = "fires-raw"

# ── Migration ──
prefixes = ["viirs/", "enriched/"]

for prefix in prefixes:
    print(f"\n📂 Migration de '{prefix}'...")
    objects = list(minio_client.list_objects(BUCKET, prefix=prefix, recursive=True))
    print(f"   {len(objects)} fichiers trouvés")
    
    for obj in objects:
        try:
            # Télécharger depuis MinIO
            response = minio_client.get_object(BUCKET, obj.object_name)
            data = response.read()
            response.close()
            response.release_conn()

            # Uploader vers Supabase
            supabase.storage.from_(BUCKET).upload(
                path=obj.object_name,
                file=data,
                file_options={"content-type": "text/csv", "upsert": "true"}
            )
            print(f"   ✅ {obj.object_name}")
        except Exception as e:
            print(f"   ❌ {obj.object_name} → {e}")

print("\n🎉 Migration terminée !")