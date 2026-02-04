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

# 1. Fix GenerateFlowFile
gen_proc = get_processor_by_name(root_pg_id, "Generate CSV Data")
if gen_proc:
    print(f"Fixing GenerateFlowFile: {gen_proc['component']['id']}")
    props = gen_proc["component"]["config"]["properties"]
    
    # Check if we have the dynamic property "Custom Text"
    custom_text_value = props.get("Custom Text")
    
    if custom_text_value:
        print(f"Found dynamic property 'Custom Text' with value. Moving to 'generate-ff-custom-text'.")
        data = {
            "revision": gen_proc["revision"],
            "component": {
                "id": gen_proc["component"]["id"],
                "config": {
                    "properties": {
                        "generate-ff-custom-text": custom_text_value,
                        "Custom Text": None # Delete dynamic property
                    }
                }
            }
        }
        resp = requests.put(f"{BASE_URL}/processors/{gen_proc['component']['id']}", json=data, headers=headers, verify=False)
        resp.raise_for_status()
        print("GenerateFlowFile fixed.")
    else:
        print("Dynamic 'Custom Text' not found (or already fixed).")

# 2. Fix EvaluateJsonPath
eval_proc = get_processor_by_name(root_pg_id, "Extract Attributes")
if eval_proc:
    print(f"Fixing EvaluateJsonPath: {eval_proc['component']['id']}")
    props = eval_proc["component"]["config"]["properties"]
    
    # Check if we have dynamic property "Destination"
    dest_value = props.get("Destination")
    
    if dest_value:
        print(f"Found dynamic property 'Destination'. Moving to 'destination'.")
        data = {
            "revision": eval_proc["revision"],
            "component": {
                "id": eval_proc["component"]["id"],
                "config": {
                    "properties": {
                        "destination": dest_value,
                        "Destination": None # Delete dynamic property
                    }
                }
            }
        }
        resp = requests.put(f"{BASE_URL}/processors/{eval_proc['component']['id']}", json=data, headers=headers, verify=False)
        resp.raise_for_status()
        print("EvaluateJsonPath fixed.")
    else:
        print("Dynamic 'Destination' not found (or already fixed).")

print("All fixes applied.")
