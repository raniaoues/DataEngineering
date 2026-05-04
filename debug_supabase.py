# debug_supabase.py
import requests

SUPABASE_URL = "https://oufkevtlizjdawzwxncr.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im91ZmtldnRsaXpqZGF3end4bmNyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NzgwODI4NywiZXhwIjoyMDkzMzg0Mjg3fQ.n2BT_Bv-pBkMKgdT0cUvINblFkdQfBybt60VNSnIoUs"

BUCKET = "fires-raw"

headers = {
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "apikey": SUPABASE_KEY,
}

# Test 1 — lister enriched
r = requests.post(
    f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET}",
    headers={**headers, "Content-Type": "application/json"},
    json={"prefix": "enriched/", "limit": 100, "offset": 0}
)
print("=== enriched/ ===")
print("Status:", r.status_code)
print("Response:", r.text)

# Test 2 — télécharger un fichier directement
r2 = requests.get(
    f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/enriched/2026/04/21/214753.csv",
    headers=headers
)
print("\n=== download test ===")
print("Status:", r2.status_code)
print("Response:", r2.text[:200])