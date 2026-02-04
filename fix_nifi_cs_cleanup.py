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
    print("CSVReader not found")
    exit(1)

# Cleanup properties
# Error says: 'csv-format' is invalid (dynamic). The real property name is likely 'CSV Format'.
# And 'Schema Access Strategy' is invalid (dynamic). The real property name is 'schema-access-strategy'.
# Wait, 'csv-format' in my logs showed value 'rfc-4180'. 
# In the output 'Properties': {'CSV Format': 'custom', 'csv-format': 'rfc-4180'}
# So 'CSV Format' is the real one, and it is set to 'custom' by default?
# I want to set the REAL 'CSV Format' to 'rfc-4180' (if that is a valid value, or whatever the enum value is)
# Actually, looking at standard CSVReader:
# Property Name: "CSV Format" -> key "CSV Format" (spaces? NiFi uses Display names as keys sometimes? or are keys internal?)
# Wait, the error ` 'csv-format' validated against 'rfc-4180' is invalid ` implies 'csv-format' is TREATED as a dynamic property because the component does not recognize it as a supported property key.
# This means the API expects the DISPLAY NAME or the exact Internal KEY.
# For 'CSV Format', the key might be "CSV Format".
# For 'Schema Access Strategy', the key is "schema-access-strategy".

# Let's try to set "CSV Format" to "rfc-4180" and remove "csv-format".
# And remove "Schema Access Strategy" (dynamic) which I presumably failed to remove or re-added?
# In fix_nifi_cs.py, I set "Schema Access Strategy": None. Did it not work?
# The output says "Schema Access Strategy": 'infer-schema' is still there? 
# Maybe I had a typo or it didn't update.
# Let's try to be very explicit.

data = {
    "revision": csv_reader["revision"],
    "component": {
        "id": csv_reader["component"]["id"],
        "config": {
            "properties": {
                # Fix Schema Strategy (ensure correct key is set, incorrect key is removed)
                "schema-access-strategy": "infer-schema",
                "Schema Access Strategy": None,
                
                # Fix CSV Format
                "CSV Format": "rfc-4180", # Try setting the key that appears in the 'Properties' list as the 'real' one
                "csv-format": None
            }
        }
    }
}

resp = requests.put(f"{BASE_URL}/controller-services/{csv_reader['component']['id']}", json=data, headers=headers, verify=False)
if resp.status_code != 200:
    print(resp.text)
resp.raise_for_status()

print("Cleaned up CSVReader properties.")

# Re-enable
csv_reader = resp.json()
data_enable = {
    "revision": csv_reader["revision"],
    "component": {
        "id": csv_reader["component"]["id"],
        "state": "ENABLED"
    }
}
resp = requests.put(f"{BASE_URL}/controller-services/{csv_reader['component']['id']}", json=data_enable, headers=headers, verify=False)
resp.raise_for_status()
print("CSVReader enabled.")
