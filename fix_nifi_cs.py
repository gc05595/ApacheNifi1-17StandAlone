import requests
import json
import urllib3

# Disable self-signed certificate warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://localhost:9443/nifi-api"
USERNAME = "admin"
PASSWORD = "password12345678"

def get_token():
    resp = requests.post(f"{BASE_URL}/access/token", data={"username": USERNAME, "password": PASSWORD}, verify=False)
    resp.raise_for_status()
    return resp.text

token = get_token()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def get_root_pg():
    resp = requests.get(f"{BASE_URL}/flow/process-groups/root", headers=headers, verify=False)
    resp.raise_for_status()
    return resp.json()["processGroupFlow"]["id"]

root_pg_id = get_root_pg()

# Find the Controller Service
def get_service_by_name(pg_id, name):
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{pg_id}/controller-services", headers=headers, verify=False)
    resp.raise_for_status()
    services = resp.json()["controllerServices"]
    for svc in services:
        if svc["component"]["name"] == name:
            return svc
    return None

csv_reader = get_service_by_name(root_pg_id, "CSVReader-Standard")

if not csv_reader:
    print("Could not find service 'CSVReader-Standard'")
    exit(1)

print(f"Found service: {csv_reader['component']['id']}")

# Update the service configuration
# Correct keys: 'schema-access-strategy' and 'csv-format' instead of Display Names.
# 'csv-format' IS the internal name (custom UI uses strictly that), but 'Schema Access Strategy' is definitely wrong.

data = {
    "revision": csv_reader["revision"],
    "component": {
        "id": csv_reader["component"]["id"],
        "config": {
            "properties": {
                "schema-access-strategy": "infer-schema",
                "Schema Access Strategy": None  # Remove the wrong one
            }
        }
    }
}

resp = requests.put(f"{BASE_URL}/controller-services/{csv_reader['component']['id']}", json=data, headers=headers, verify=False)
if resp.status_code != 200:
    print(resp.text)
resp.raise_for_status()
print("Service configuration updated.")

# Retrieve updated revision to enable it
csv_reader_updated = resp.json()

# Enable the service
data_enable = {
    "revision": csv_reader_updated["revision"],
    "component": {
        "id": csv_reader_updated["component"]["id"],
        "state": "ENABLED"
    }
}

resp = requests.put(f"{BASE_URL}/controller-services/{csv_reader['component']['id']}", json=data_enable, headers=headers, verify=False)
resp.raise_for_status()

print("CSVReader enabled successfully!")
