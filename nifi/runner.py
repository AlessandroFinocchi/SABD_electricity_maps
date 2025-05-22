import requests
import urllib3

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

nifi_host = "localhost"
nifi_port = "8443"
clientId = "b9112890-f366-44d4-a0d3-6ed7f4d53cec"
pg_id = "6f8f46ad-5017-32f9-8d71-b68fca956eb3"

base_url = f"https://{nifi_host}:{nifi_port}"

# Step 1: Get JWT token
auth_url = f"{base_url}/nifi-api/access/token"
auth_data = {
    "username": "admin",
    "password": "ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
}
headers_auth = {
    "Content-Type": "application/x-www-form-urlencoded"
}

response = requests.post(auth_url, headers=headers_auth, data=auth_data, verify=False)
response.raise_for_status()
jwt = response.text  # The token is returned as plain text

# Step 2: Get root process group ID
root_pg_url = f"{base_url}/nifi-api/process-groups/root"
headers_authz = {
    "Authorization": f"Bearer {jwt}"
}
response = requests.get(root_pg_url, headers=headers_authz, verify=False)
response.raise_for_status()
root_pg_json = response.json()
root_pg = root_pg_json["id"]

# Step 3: Set root process group state to RUNNING
put_url = f"{base_url}/nifi-api/flow/process-groups/{root_pg}"
headers_put = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {jwt}"
}
payload = {
    "id": root_pg,
    "disconnectedNodeAcknowledged": False,
    "state": "RUNNING"
}

response = requests.put(put_url, headers=headers_put, json=payload, verify=False)
response.raise_for_status()

print("Root process group state set to RUNNING successfully.")
