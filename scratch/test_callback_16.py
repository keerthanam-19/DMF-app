import requests
import json

session = requests.Session()
r_deps = session.get('http://127.0.0.1:8050/_dash-dependencies')
deps = r_deps.json()

target_cb = deps[20]

payload = {
    "output": target_cb['output'],
    "outputs": target_cb['output'],
    "inputs": [
        {"id": "run-all-checks-btn", "property": "n_clicks", "value": None},
        {"id": "header-run-validation-btn", "property": "n_clicks", "value": 1},
        {"id": "run-all-checks-btn-quick", "property": "n_clicks", "value": None},
        {"id": '{"index":["ALL"],"type":"dmf-delete-btn"}', "property": "n_clicks", "value": []},
        {"id": '{"index":["ALL"],"type":"dmf-rerun-btn"}', "property": "n_clicks", "value": []}
    ],
    "changedPropIds": ["header-run-validation-btn.n_clicks"],
    "state": [
        {"id": "dmf-refresh-store", "property": "data", "value": 0},
        {"id": "sync-btn", "property": "n_clicks", "value": 0}
    ]
}

print("Triggering Callback 20 (Run Validation Engine)...")
res = session.post(
    'http://127.0.0.1:8050/_dash-update-component',
    json=payload,
    headers={'Content-Type': 'application/json'}
)

print("Status code:", res.status_code)
res_json = res.json()
print("Keys:", res_json.keys())
print("Response JSON:")
print(json.dumps(res_json, indent=2))
