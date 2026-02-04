import requests
import json
import urllib3
import time

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
print(f"Root PG ID: {root_pg_id}")

def create_controller_service(pg_id, type_class, name, properties):
    data = {
        "revision": {"version": 0},
        "component": {
            "type": type_class,
            "name": name,
            "properties": properties
        }
    }
    resp = requests.post(f"{BASE_URL}/process-groups/{pg_id}/controller-services", json=data, headers=headers, verify=False)
    resp.raise_for_status()
    return resp.json()

def enable_controller_service(service_id, version):
    data = {
        "revision": {"version": version},
        "component": {
            "id": service_id,
            "state": "ENABLED"
        }
    }
    resp = requests.put(f"{BASE_URL}/controller-services/{service_id}", json=data, headers=headers, verify=False)
    resp.raise_for_status()
    return resp.json()

# Create CSVReader
csv_reader = create_controller_service(root_pg_id, "org.apache.nifi.csv.CSVReader", "CSVReader-Standard", {
    "Schema Access Strategy": "infer-schema",
    "csv-format": "rfc-4180"
})
print("Created CSVReader")

# Create JsonRecordSetWriter
json_writer = create_controller_service(root_pg_id, "org.apache.nifi.json.JsonRecordSetWriter", "JsonRecordSetWriter-Standard", {})
print("Created JsonRecordSetWriter")

# Enable Services (Wait a bit?)
enable_controller_service(csv_reader["component"]["id"], csv_reader["revision"]["version"])
enable_controller_service(json_writer["component"]["id"], json_writer["revision"]["version"])
print("Enabled Controller Services")

def create_processor(pg_id, type_class, name, position, properties=None, config=None):
    data = {
        "revision": {"version": 0},
        "component": {
            "type": type_class,
            "name": name,
            "position": position,
            "config": config if config else {}
        }
    }
    if properties:
        data["component"]["config"]["properties"] = properties
        
    resp = requests.post(f"{BASE_URL}/process-groups/{pg_id}/processors", json=data, headers=headers, verify=False)
    resp.raise_for_status()
    return resp.json()

# 1. GenerateFlowFile
gen_flow = create_processor(root_pg_id, "org.apache.nifi.processors.standard.GenerateFlowFile", "Generate CSV Data", {"x": 300, "y": 0}, {
    "Custom Text": "id,name,role\n1,Gonzalo,Admin\n2,AntiGravity,AI\n3,NiFi,Tool"
})
print("Created GenerateFlowFile")

# 2. ConvertRecord
convert_record = create_processor(root_pg_id, "org.apache.nifi.processors.standard.ConvertRecord", "Convert CSV to JSON", {"x": 300, "y": 200}, {
    "Record Reader": csv_reader["component"]["id"],
    "Record Writer": json_writer["component"]["id"]
})
print("Created ConvertRecord")

# 3. EvaluateJsonPath
evaluate_json = create_processor(root_pg_id, "org.apache.nifi.processors.standard.EvaluateJsonPath", "Extract Attributes", {"x": 300, "y": 400}, {
    "Destination": "flowfile-attribute",
    "record_id": "$.id",
    "record_name": "$.name",
    "record_role": "$.role"
})
print("Created EvaluateJsonPath")

# 4. LogAttribute
log_attr = create_processor(root_pg_id, "org.apache.nifi.processors.standard.LogAttribute", "Log Results", {"x": 300, "y": 600})
print("Created LogAttribute")

def create_connection(pg_id, source, destination, relationships):
    data = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": source["component"]["id"], "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": destination["component"]["id"], "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": relationships
        }
    }
    resp = requests.post(f"{BASE_URL}/process-groups/{pg_id}/connections", json=data, headers=headers, verify=False)
    resp.raise_for_status()
    return resp.json()

create_connection(root_pg_id, gen_flow, convert_record, ["success"])
create_connection(root_pg_id, convert_record, evaluate_json, ["success"])
create_connection(root_pg_id, evaluate_json, log_attr, ["matched"])
print("Created Connections")

# Auto-terminate unused
def auto_terminate(processor, relationships):
    data = {
        "revision": processor["revision"],
        "component": {
            "id": processor["component"]["id"],
            "config": {
                "autoTerminatedRelationships": relationships
            }
        }
    }
    resp = requests.put(f"{BASE_URL}/processors/{processor['component']['id']}", json=data, headers=headers, verify=False)
    resp.raise_for_status()
    return resp.json()

# Terminate failures and unmatched
auto_terminate(convert_record, ["failure"])
auto_terminate(evaluate_json, ["failure", "unmatched"])
auto_terminate(log_attr, ["success"])

print("Auto-terminated unused relationships")
print("Flow created successfully!")
