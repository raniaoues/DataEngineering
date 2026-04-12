import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Step 1: Login to get a temporary token
print(" Logging in to GFW...")
response = requests.post(
    "https://api.resourcewatch.org/auth/login",
    json={
        "email": os.getenv("GFW_EMAIL"),
        "password": os.getenv("GFW_PASSWORD")
    }
)

if response.status_code != 200:
    print(f" Login failed: {response.status_code}")
    print(response.text)
    exit()

token = response.json()["data"]["token"]
print(" Login successful, token received")

# Step 2: Create an application to get a permanent API key
print(" Creating GFW application...")
app_res = requests.post(
    "https://api.resourcewatch.org/v1/application",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    },
    json={
        "name": "fire-pipeline"
    }
)

if app_res.status_code not in [200, 201]:
    print(f"❌ App creation failed: {app_res.status_code}")
    print(app_res.text)
    exit()

api_key = app_res.json()["data"]["attributes"]["apiKeyValue"]
print(f"Your GFW API Key: {api_key}")
print(f"\nAdd this to your .env file:")
print(f"GFW_API_KEY={api_key}")