import requests
import json
import urllib3

# Disable self-signed certificate warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://localhost:9443/nifi-api"
USERNAME = "admin"
PASSWORD = "password12345678"

try:
    resp = requests.post(f"{BASE_URL}/access/token", data={"username": USERNAME, "password": PASSWORD}, verify=False)
    resp.raise_for_status()
    token = resp.text
except Exception as e:
    print(f"Auth failed: {e}")
    exit(1)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

resp = requests.get(f"{BASE_URL}/flow/process-groups/root", headers=headers, verify=False)
resp.raise_for_status()
flow = resp.json()["processGroupFlow"]["flow"]

found_split = False
for proc in flow["processors"]:
    if proc["component"]["name"] == "Split JSON Array":
        found_split = True
        print("Found 'Split JSON Array' processor.")
        break

if found_split:
    print("Persistence Verified: Flow is present.")
else:
    print("Persistence FAILED: Flow not found.")
