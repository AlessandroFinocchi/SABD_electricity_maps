import requests
import urllib3
import os

from enum import Enum


NIFI_HOST = "nifi"
NIFI_PORT = "8443"
CLIENT_ID = "b9112890-f366-44d4-a0d3-6ed7f4d53cec"
PG_ID     = "6f8f46ad-5017-32f9-8d71-b68fca956eb3"
BASE_URL  = f"https://{NIFI_HOST}:{NIFI_PORT}"
PG_LEN    = 11

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class State(Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"

def get_jwt() -> str:
    auth_url = f"{BASE_URL}/nifi-api/access/token"
    auth_data = {
        "username": os.environ['NIFI_USR'],
        "password": os.environ['NIFI_PSW']
    }
    headers_auth = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = requests.post(auth_url, headers=headers_auth, data=auth_data, verify=False)
    response.raise_for_status()
    jwt = response.text  # The token is returned as plain text

    return jwt

def get_root_pg_id(jwt:str) -> str:
    root_pg_url = f"{BASE_URL}/nifi-api/process-groups/root"
    headers_authz = {
        "Authorization": f"Bearer {jwt}"
    }
    response = requests.get(root_pg_url, headers=headers_authz, verify=False)
    response.raise_for_status()
    root_pg_json = response.json()
    root_pg = root_pg_json["id"]

    return root_pg

def is_nifi_running(jwt: str, root_pg: str) -> bool:
    url = f"https://{NIFI_HOST}:{NIFI_PORT}/nifi-api/flow/process-groups/{root_pg}/status?recursive=true"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt}"
    }

    resp = requests.get(url, headers=headers, verify=False)
    resp.raise_for_status()
    data = resp.json()

    agg = data.get("processGroupStatus", {}) \
        .get("aggregateSnapshot", {})

    # first, try nested PG snapshots
    nested = agg.get("processGroupStatusSnapshots", [])
    if nested:
        # take the first nested group's snapshots
        snapshots = (
            nested[0]
            .get("processGroupStatusSnapshot", {})
            .get("processorStatusSnapshots", [])
        )
    else:
        # fall back to this group's own snapshots
        snapshots = agg.get("processorStatusSnapshots", [])

    # count how many are Running
    running_count = sum(
        1
        for s in snapshots
        if s.get("processorStatusSnapshot", s).get("runStatus") == "Running"
    )

    if running_count == 0:        return False
    elif running_count == PG_LEN: return True
    else: raise Exception(f"Unexpected number of running processors: {running_count}")

def set_pg_state(jwt:str, root_pg:str, state:State) -> None:
    put_url = f"{BASE_URL}/nifi-api/flow/process-groups/{root_pg}"
    headers_put = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt}"
    }
    payload = {
        "id": root_pg,
        "disconnectedNodeAcknowledged": False,
        "state": state.value
    }

    response = requests.put(put_url, headers=headers_put, json=payload, verify=False)
    response.raise_for_status()

    print(f"Root process group state set to {state.value} successfully.")

def run_nifi_flow():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    jwt = get_jwt()
    root_pg = get_root_pg_id(jwt)
    if is_nifi_running(jwt, root_pg):
        set_pg_state(jwt, root_pg, State.RUNNING)

def stop_nifi_flow():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    jwt = get_jwt()
    root_pg = get_root_pg_id(jwt)
    if not is_nifi_running(jwt, root_pg):
        set_pg_state(jwt, root_pg, State.STOPPED)