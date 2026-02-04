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

def get_service_by_name(pg_id, name):
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{pg_id}/controller-services", headers=headers, verify=False)
    resp.raise_for_status()
    services = resp.json()["controllerServices"]
    for svc in services:
        if svc["component"]["name"] == name:
            return svc
    return None

csv_reader = get_service_by_name(root_pg_id, "CSVReader-Standard")

if csv_reader:
    print(f"Service: {csv_reader['component']['name']}")
    print(f"State: {csv_reader['component']['state']}")
    print(f"Validation Status: {csv_reader['component']['validationStatus']}")
    if 'validationErrors' in csv_reader['component']:
        print("Validation Errors:")
        for err in csv_reader['component']['validationErrors']:
            print(f"- {err}")
    
    print("\nProperties:")
    pprint.pprint(csv_reader['component']['properties'])
else:
    print("CSVReader not found")
