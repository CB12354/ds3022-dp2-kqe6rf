"""DS 3022 Data Project 2: setup check.

Run after starting the emulator (and ideally the scatter API):

    python kit/smoke_test.py

Prints "Emulator OK" when your laptop is ready for Project 2.
"""
from __future__ import annotations

import importlib
import json
import os
import sys
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _dp2  # noqa: E402

OK, WARN, BAD = "[ OK ]", "[WARN]", "[FAIL]"
failures = 0


def report(status, msg):
    global failures
    failures += status == BAD
    print(f"{status} {msg}")


# 1. Python and packages
if sys.version_info < (3, 10):
    report(BAD, f"Python {sys.version.split()[0]}: please use Python 3.12 or 3.13 (see SETUP.md)")
else:
    report(OK, f"Python {sys.version.split()[0]}")

for pkg, required in (("boto3", True), ("requests", True), ("duckdb", True), ("prefect", True), ("dbt.cli.main", True), ("dotenv", False)):
    try:
        importlib.import_module(pkg)
        report(OK, f"import {pkg}")
    except Exception as exc:  # noqa: BLE001
        report(BAD if required else WARN, f"import {pkg} ({exc.__class__.__name__}). Run: pip install -r requirements.txt")

# 2. Environment
report(OK, f"AWS_ENDPOINT_URL_SQS = {_dp2.endpoint()}")
if "amazonaws.com" in _dp2.endpoint():
    report(WARN, "endpoint points at real AWS, not the local emulator")

# 3. Emulator round trip
try:
    sqs = _dp2.sqs_client()
    name = f"smoke-{uuid.uuid4().hex[:8]}"
    url = sqs.create_queue(QueueName=name)["QueueUrl"]
    sqs.send_message(
        QueueUrl=url,
        MessageBody="ping",
        MessageAttributes={"word": {"DataType": "String", "StringValue": "hello"}},
    )
    resp = sqs.receive_message(QueueUrl=url, MessageAttributeNames=["All"], WaitTimeSeconds=2)
    msg = resp.get("Messages", [None])[0]
    assert msg and msg["MessageAttributes"]["word"]["StringValue"] == "hello", "message attribute not returned"
    sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
    sqs.delete_queue(QueueUrl=url)
    report(OK, "SQS emulator: create / send / receive / delete")
except Exception as exc:  # noqa: BLE001
    report(BAD, f"SQS emulator not reachable or not working at {_dp2.endpoint()}: {exc}")
    print("       Start it in T1:  moto_server -p 9324")

# 4. Scatter API (optional at this point)
port = os.environ.get("SCATTER_PORT", "8000")
try:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as r:
        info = json.load(r)
    report(OK, f"scatter API on port {port} (fast mode: {info.get('fast_mode')})")
except Exception:  # noqa: BLE001
    report(WARN, f"scatter API not answering on port {port}. Start it in T2:  python kit/scatter_api.py")

print()
if failures:
    print(f"{failures} problem(s) found. Fix the [FAIL] lines above and run again.")
    sys.exit(1)
print("Emulator OK")
