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

# Find the ConvertRecord processor
def get_processor_by_name(pg_id, name):
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{pg_id}", headers=headers, verify=False)
    resp.raise_for_status()
    flow = resp.json()["processGroupFlow"]["flow"]
    for proc in flow["processors"]:
        if proc["component"]["name"] == name:
            return proc
    return None

convert_proc = get_processor_by_name(root_pg_id, "Convert CSV to JSON")

if not convert_proc:
    print("Could not find processor 'Convert CSV to JSON'")
    exit(1)

print(f"Found processor: {convert_proc['component']['id']}")

# Get current properties to find the service IDs we mistakenly put in dynamic properties
props = convert_proc["component"]["config"]["properties"]
reader_id = props.get("Record Reader")
writer_id = props.get("Record Writer")

if not reader_id or not writer_id:
    # If not found by name, try looking up the services directly? 
    # Or maybe the user partially fixed it?
    # Let's assume the screenshot state: they are there.
    print("Could not retrieve Service IDs from dynamic properties. Attempting to fetch Controller Services...")
    # Fetch CS list
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{root_pg_id}/controller-services", headers=headers, verify=False)
    services = resp.json()["controllerServices"]
    for svc in services:
        if svc["component"]["name"] == "CSVReader-Standard":
            reader_id = svc["component"]["id"]
        elif svc["component"]["name"] == "JsonRecordSetWriter-Standard":
            writer_id = svc["component"]["id"]

print(f"Reader ID: {reader_id}")
print(f"Writer ID: {writer_id}")

# Update the processor
# We need to set 'record-reader' and 'record-writer' (kebab case usually, or camelCase?)
# Checking NiFi docs/standard practices: likely `record-reader` and `record-writer`.
# AND we need to delete "Record Reader" and "Record Writer" (set to null).

data = {
    "revision": convert_proc["revision"],
    "component": {
        "id": convert_proc["component"]["id"],
        "config": {
            "properties": {
                "record-reader": reader_id,
                "record-writer": writer_id,
                "Record Reader": None,
                "Record Writer": None
            }
        }
    }
}

resp = requests.put(f"{BASE_URL}/processors/{convert_proc['component']['id']}", json=data, headers=headers, verify=False)
if resp.status_code != 200:
    print(resp.text)
resp.raise_for_status()

print("Processor configuration updated successfully!")
