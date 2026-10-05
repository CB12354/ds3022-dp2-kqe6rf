"""Shared helpers for the DS 3022 Data Project 2 kit.

Settings, the SQS client, your phrase and receipt codes, used by
scatter_api.py, check_submission.py and smoke_test.py.

You don't need to open or change this file: your own code goes in
prefect-flow.py and dbt/.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import zlib
from pathlib import Path

import boto3

try:  # load .env from the repo root if python-dotenv is installed
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:  # pragma: no cover
    pass

# Sensible local defaults so the kit works even without a .env file.
os.environ.setdefault("AWS_ENDPOINT_URL_SQS", "http://127.0.0.1:9324")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

N_FRAGMENTS = 21
SUBMIT_QUEUE = "dp2-submit"
UVA_ID_RE = re.compile(r"^[a-z][a-z0-9]{1,15}$")


# Encoded phrase bank. Leave it as it is: changing it changes your phrase
# and makes your receipt code invalid.
_BANK = (
    "eNpdU8uu1TAM/BXrriv+AQmEWIIQG8TCJ3HbqGlc2cmp+vdM0oOuYNc4jufh6a+3T1yZpCypiJgT"
    "m1BdhY7c9kcv6DzOsbedajlOtPOWykLe0CtPsYui6dE7U5nVdq5JC5lwWMXvaaJHFjpXJcBESvXD"
    "20RvH+lIh2RA06WNApeiFQ+tFXKeJV+UnHqN3zsZp5wnzOiXTB4sHRUwuFj5OKQAoGonu5GWIAPq"
    "W5MGMln6rJm90mEaWxAjLrGPyXpS0OJtRy2nJ3zQRcDeQJ+DzC3na6KHBG4uYPXQeA39gPWByOAE"
    "AzDxui+z3+hfVMFJvDpFHYKA/hyq7bYWUoKaSaiAaF2OXLSJHMO/XUFY9gebsXs3f09eeYOimYF3"
    "8kWz6X4PBD3zgftdON4DxJ0XmWDrU/6t4FAoCpz57+KvUqYA1BXLRamegnZfteWIXXb1Wf2lZGWL"
    "w/cB/nlEQ62noBpXGAPVIA7WJtWSOPjgNraMUl9D1gW1S4b+nqBMvqWcuz1b0bMrP9cUVshOGfmD"
    "nwK1z87SBeuLFFbG0vs+vIWAtA0yXwv95CwlJL4TyZIzj5X52Nm9z2nQKJLG3qN2Xt0cvhCqlzNg"
    "AAv6uzXFiM+aemTmVJDLixBe/DV9hVrSst5J/3FKqTAD+Z2Nlx2n/q8ZYha7sZ3SaQp5alFseoXy"
    "wK+E1vfw12Yj3qvs9OCw4S2E9rEhC99ZDrofY5uOpzJ+gN9/ANM6ZuA="
)


def _phrases() -> list[str]:
    return json.loads(zlib.decompress(base64.b64decode(_BANK)).decode())


def phrase_for(uvaid: str) -> str:
    """Deterministic phrase for a computing ID (same ID -> same phrase)."""
    bank = _phrases()
    idx = int(hashlib.sha256(uvaid.encode()).hexdigest(), 16) % len(bank)
    return bank[idx]


def receipt_for(uvaid: str, phrase: str, platform: str) -> str:
    """Short receipt code proving a correct submission was checked."""
    key = b"ds3022-fall2026-dp2"
    msg = f"{uvaid}|{platform}|{' '.join(phrase.split())}".encode()
    return hmac.new(key, msg, hashlib.sha256).hexdigest()[:10].upper()


def sqs_client():
    """boto3 SQS client. The endpoint comes from AWS_ENDPOINT_URL_SQS."""
    return boto3.client("sqs")


def endpoint() -> str:
    return os.environ["AWS_ENDPOINT_URL_SQS"]
