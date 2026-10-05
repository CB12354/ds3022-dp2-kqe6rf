"""DS 3022 Data Project 2: local scatter API.

Fills your SQS queue with the 21 puzzle messages. Run it in its own terminal
(T2) while the SQS emulator is running in T1:

    python kit/scatter_api.py

Your pipeline then calls it with an HTTP POST:

    POST http://127.0.0.1:8000/api/scatter/<UVA_ID>

Each call creates (or empties) the queue named <UVA_ID>, makes sure the
submission queue 'dp2-submit' exists, and sends exactly 21 messages in random
order. Each message has a random DelaySeconds:

    normal mode : 30-900 seconds (the real assignment)
    DP2_FAST=1  : 5-60 seconds   (set this in .env while you develop)

Chaos mode (DP2_CHAOS=1 in .env, used in Lesson 11) imitates what a real SQS
standard queue is allowed to do: on top of the 21 fragments it sends
2 duplicate copies of fragments (at-least-once delivery) and 1 malformed
"poison" message with no 'word' attribute. Your pipeline should still store 21
distinct fragments, delete every message, and submit the right phrase.

Response JSON:  {"hello": "<UVA_ID>", "sqs_url": "<queue url>", "messages": 21, "mode": "..."}

You don't need to change this file. It reads .env only when it starts, so
after changing DP2_FAST or DP2_CHAOS press Ctrl+C and start it again.
"""
from __future__ import annotations

import json
import os
import random
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _dp2  # noqa: E402

PORT = int(os.environ.get("SCATTER_PORT", "8000"))
HOST = os.environ.get("SCATTER_HOST", "127.0.0.1")  # 0.0.0.0 to accept connections from other machines


def fast_mode() -> bool:
    return os.environ.get("DP2_FAST", "0").strip().lower() in {"1", "true", "yes"}


def chaos_mode() -> bool:
    return os.environ.get("DP2_CHAOS", "0").strip().lower() in {"1", "true", "yes"}


def scatter(uvaid: str) -> dict:
    sqs = _dp2.sqs_client()
    queue_url = sqs.create_queue(QueueName=uvaid)["QueueUrl"]
    sqs.create_queue(QueueName=_dp2.SUBMIT_QUEUE)

    # Empty the queue so every POST starts a fresh puzzle.
    sqs.purge_queue(QueueUrl=queue_url)

    words = _dp2.phrase_for(uvaid).split()
    assert len(words) == _dp2.N_FRAGMENTS
    lo, hi = (5, 60) if fast_mode() else (30, 900)

    fragments = list(enumerate(words, start=1))  # order_no starts at 1
    messages = [
        {"order_no": {"DataType": "String", "StringValue": str(o)},
         "word": {"DataType": "String", "StringValue": w}}
        for o, w in fragments
    ]
    if chaos_mode():
        messages += random.sample(messages, 2)  # duplicates: same fragment delivered twice
        messages.append({"order_no": {"DataType": "String", "StringValue": "?"}})  # poison: no 'word'
    random.shuffle(messages)
    for attrs in messages:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="puzzle",  # meaningless on purpose: the data is in the attributes
            DelaySeconds=random.randint(lo, hi),
            MessageAttributes=attrs,
        )
    mode = f"fast ({lo}-{hi}s delays)" if fast_mode() else f"normal ({lo}-{hi}s delays)"
    return {
        "hello": uvaid,
        "sqs_url": queue_url,
        "messages": len(messages),
        "mode": mode + (" + CHAOS" if chaos_mode() else ""),
    }


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # health check used by smoke_test.py
        if self.path.rstrip("/") in {"", "/health"}:
            self._send(200, {"status": "ok", "sqs_endpoint": _dp2.endpoint(), "fast_mode": fast_mode()})
        elif self.path.startswith("/api/scatter/"):
            self._send(405, {"error": "Use HTTP POST, not GET, for /api/scatter/<UVA_ID>"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        prefix = "/api/scatter/"
        if not self.path.startswith(prefix):
            self._send(404, {"error": "POST to /api/scatter/<UVA_ID>"})
            return
        uvaid = self.path[len(prefix):].strip("/").lower()
        if not _dp2.UVA_ID_RE.match(uvaid):
            self._send(400, {"error": f"'{uvaid}' does not look like a computing ID (e.g. mst3k)"})
            return
        try:
            result = scatter(uvaid)
        except EndpointConnectionError:
            self._send(503, {"error": f"Cannot reach the SQS emulator at {_dp2.endpoint()}. Is it running?"})
            return
        except (ClientError, BotoCoreError) as exc:
            self._send(500, {"error": str(exc)})
            return
        print(f"scattered 21 messages for {uvaid} ({result['mode']})", flush=True)
        self._send(200, result)

    def log_message(self, fmt, *args):  # quieter console
        sys.stderr.write(f"[scatter-api] {self.address_string()} {fmt % args}\n")


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    mode = ("FAST (5-60s)" if fast_mode() else "NORMAL (30-900s)") + (" + CHAOS" if chaos_mode() else "")
    print(f"Scatter API on http://127.0.0.1:{PORT}  |  SQS emulator: {_dp2.endpoint()}  |  delays: {mode}")
    print("POST http://127.0.0.1:%d/api/scatter/<UVA_ID>   (Ctrl+C to stop)" % PORT, flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
