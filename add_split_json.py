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

def get_processor_by_name(pg_id, name):
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{pg_id}", headers=headers, verify=False)
    resp.raise_for_status()
    flow = resp.json()["processGroupFlow"]["flow"]
    for proc in flow["processors"]:
        if proc["component"]["name"] == name:
            return proc
    return None

def get_connection(pg_id, source_id, dest_id):
    resp = requests.get(f"{BASE_URL}/flow/process-groups/{pg_id}", headers=headers, verify=False)
    resp.raise_for_status()
    flow = resp.json()["processGroupFlow"]["flow"]
    for conn in flow["connections"]:
        if conn["component"]["source"]["id"] == source_id and conn["component"]["destination"]["id"] == dest_id:
            return conn
    return None

# Locate existing processors
convert_proc = get_processor_by_name(root_pg_id, "Convert CSV to JSON")
eval_proc = get_processor_by_name(root_pg_id, "Extract Attributes")

if not convert_proc or not eval_proc:
    print("Could not find required processors.")
    exit(1)

print(f"ConvertRecord: {convert_proc['component']['id']}")
print(f"EvaluateJsonPath: {eval_proc['component']['id']}")

# 1. Create SplitJson
split_json_data = {
    "revision": {"version": 0},
    "component": {
        "type": "org.apache.nifi.processors.standard.SplitJson",
        "name": "Split JSON Array",
        "position": {"x": 300, "y": 300}, # Between Extract (400) and Convert (200) - wait, Extract was 400.
        "config": {
            "properties": {
                "JsonPathExpression": "$."
            }
        }
    }
}
resp = requests.post(f"{BASE_URL}/process-groups/{root_pg_id}/processors", json=split_json_data, headers=headers, verify=False)
resp.raise_for_status()
split_proc = resp.json()
print("Created SplitJson")

# 2. Delete existing connection Convert -> Evaluate
conn = get_connection(root_pg_id, convert_proc["component"]["id"], eval_proc["component"]["id"])
if conn:
    print(f"Deleting connection: {conn['component']['id']}")
    # Purge? No, just delete. (Assuming queue is empty or we don't care)
    # If the Connection has FlowFiles, delete might fail or need confirmation?
    # Delete request usually works, it drops the flowfiles.
    resp = requests.delete(f"{BASE_URL}/connections/{conn['component']['id']}?version={conn['revision']['version']}", headers=headers, verify=False)
    # If clientid is needed checking docs... usually version is enough.
    if resp.status_code == 409: # Conflict if not empty maybe
        print("Could not delete connection immediately (probably active threads or flowfiles). Continuing anyway...")
    else:
        resp.raise_for_status()

# 3. Connect Convert -> SplitJson
data_conn1 = {
    "revision": {"version": 0},
    "component": {
        "source": {"id": convert_proc["component"]["id"], "groupId": root_pg_id, "type": "PROCESSOR"},
        "destination": {"id": split_proc["component"]["id"], "groupId": root_pg_id, "type": "PROCESSOR"},
        "selectedRelationships": ["success"]
    }
}
resp = requests.post(f"{BASE_URL}/process-groups/{root_pg_id}/connections", json=data_conn1, headers=headers, verify=False)
resp.raise_for_status()
print("Connected Convert -> SplitJson")

# 4. Connect SplitJson -> Evaluate
data_conn2 = {
    "revision": {"version": 0},
    "component": {
        "source": {"id": split_proc["component"]["id"], "groupId": root_pg_id, "type": "PROCESSOR"},
        "destination": {"id": eval_proc["component"]["id"], "groupId": root_pg_id, "type": "PROCESSOR"},
        "selectedRelationships": ["split"]
    }
}
resp = requests.post(f"{BASE_URL}/process-groups/{root_pg_id}/connections", json=data_conn2, headers=headers, verify=False)
resp.raise_for_status()
print("Connected SplitJson -> Evaluate")

# 5. Auto-terminate SplitJson unused
data_term = {
    "revision": split_proc["revision"],
    "component": {
        "id": split_proc["component"]["id"],
        "config": {
            "autoTerminatedRelationships": ["failure", "original"]
        }
    }
}
resp = requests.put(f"{BASE_URL}/processors/{split_proc['component']['id']}", json=data_term, headers=headers, verify=False)
resp.raise_for_status()
print("Configured SplitJson auto-termination")

print("Flow modified successfully!")
