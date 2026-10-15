"""Exercise an independently running API and worker using synthetic sales."""

import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
raw = os.getenv("TENANT_KEYS_JSON", "")
if not raw:
    for line in (ROOT / ".env").read_text().splitlines():
        if line.startswith("TENANT_KEYS_JSON="):
            raw = line.split("=", 1)[1].strip("'")
KEYS = json.loads(raw)
BASE = os.getenv("BASE_URL", "http://127.0.0.1:8010")


def call(method, path, tenant="acme", body=None, key=None, expected=200):
    headers = {"X-API-Key": KEYS[tenant], "Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    request = Request(
        BASE + path, method=method, headers=headers, data=json.dumps(body).encode() if body is not None else None
    )
    try:
        response = urlopen(request, timeout=10)
    except HTTPError as error:
        response = error
    with response:
        result = json.load(response)
        if response.status != expected:
            raise RuntimeError(f"{method} {path}: expected {expected}, got {response.status}: {result}")
        return result


request_key = "demo-" + uuid4().hex
body = {"csv_data": (ROOT / "examples/sales.csv").read_text()}
job = call("POST", "/v1/jobs", body=body, key=request_key, expected=202)
assert call("POST", "/v1/jobs", body=body, key=request_key)["id"] == job["id"]
path = "/v1/jobs/" + job["id"]
call("GET", path, tenant="globex", expected=404)
call("POST", path + "/cancel", tenant="globex", expected=404)
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    job = call("GET", path)
    if job["status"] in ("succeeded", "failed"):
        break
    time.sleep(0.2)
assert job["status"] == "succeeded", job
report = job["result"]
assert report["input_rows"] == 15
assert report["accepted_rows"] == 11
assert report["rejected_rows"] == 4
assert report["outliers"] == [{"sale_id": "SALE-009", "currency": "USD", "amount_cents": 100000}]
print(
    json.dumps(
        {
            "job_status": job["status"],
            "input_rows": report["input_rows"],
            "accepted_rows": report["accepted_rows"],
            "rejected_rows": report["rejected_rows"],
            "totals": report["totals"],
        },
        indent=2,
    )
)
print(
    "PASS: asynchronous analysis, exact totals, quality report, outlier signal, idempotent upload and tenant isolation."
)
