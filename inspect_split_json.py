import requests
import json
import urllib3
import pprint

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

def get_processor_by_name(pg_id, name):
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{pg_id}", headers=headers, verify=False)
    resp.raise_for_status()
    flow = resp.json()["processGroupFlow"]["flow"]
    for proc in flow["processors"]:
        if proc["component"]["name"] == name:
            return proc
    return None

split_proc = get_processor_by_name(root_pg_id, "Split JSON Array")

if not split_proc:
    print("SplitJson not found")
    exit(1)

# Just get the processor details directly
resp = requests.get(f"{BASE_URL}/processors/{split_proc['component']['id']}", headers=headers, verify=False)
if resp.status_code != 200:
    print(f"Error fetching processor: {resp.text}")
    exit(1)

full_proc = resp.json()
print("Descriptors:")
descriptors = full_proc["component"]["config"]["descriptors"]
for key, desc in descriptors.items():
    print(f"Key: '{key}', Display Name: '{desc['displayName']}'")
