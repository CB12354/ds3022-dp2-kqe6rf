"""DS 3022 Data Project 2: check what your pipeline submitted.

Reads every message waiting in the 'dp2-submit' queue on the local emulator,
checks the 'uvaid', 'phrase' and 'platform' attributes, and prints a receipt
code for each correct submission. Paste the receipt code into your README.

    python kit/check_submission.py            # check and remove submissions
    python kit/check_submission.py --keep     # check but leave them in the queue

Check a receipt code again later, for example the one in your README:

    python kit/check_submission.py --verify mst3k prefect 359B549CEF
"""
from __future__ import annotations

import argparse
import os
import sys

from botocore.exceptions import ClientError, EndpointConnectionError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _dp2  # noqa: E402

VALID_PLATFORMS = {"prefect"}


def norm(s: str) -> str:
    return " ".join(s.split())


def check_one(attrs: dict) -> tuple[bool, str]:
    def val(name):
        return attrs.get(name, {}).get("StringValue")

    uvaid, phrase, platform = val("uvaid"), val("phrase"), val("platform")
    missing = [n for n, v in (("uvaid", uvaid), ("phrase", phrase), ("platform", platform)) if not v]
    if missing:
        return False, f"FAIL  missing message attribute(s): {', '.join(missing)}"
    uvaid, platform = uvaid.strip().lower(), platform.strip().lower()
    if platform not in VALID_PLATFORMS:
        return False, f"FAIL  {uvaid}: platform must be 'prefect', got '{platform}'"

    expected = _dp2.phrase_for(uvaid)
    if norm(phrase) == norm(expected):
        return True, f"PASS  {uvaid} / {platform}  receipt: {_dp2.receipt_for(uvaid, expected, platform)}"

    got, exp = norm(phrase).split(), expected.split()
    if sorted(got) == sorted(exp):
        return False, f"FAIL  {uvaid}: all the right words, wrong order (check how you sort order_no)"
    if len(got) != len(exp):
        return False, f"FAIL  {uvaid}: {len(got)} words submitted, expected {len(exp)} (missing or duplicated fragments?)"
    return False, f"FAIL  {uvaid}: phrase does not match"


def drain(keep: bool) -> int:
    sqs = _dp2.sqs_client()
    try:
        url = sqs.get_queue_url(QueueName=_dp2.SUBMIT_QUEUE)["QueueUrl"]
    except EndpointConnectionError:
        print(f"Cannot reach the SQS emulator at {_dp2.endpoint()}. Is it running?")
        return 2
    except ClientError:
        print("Queue 'dp2-submit' does not exist yet. Run the scatter API POST first.")
        return 2

    seen = passed = 0
    while True:
        resp = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=["All"],
            WaitTimeSeconds=1,
            VisibilityTimeout=5 if keep else 30,
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            seen += 1
            ok, line = check_one(m.get("MessageAttributes", {}))
            passed += ok
            print(line)
            if not keep:
                sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])
        if keep:
            break  # avoid re-reading the same messages
    if seen == 0:
        print("No submissions found in 'dp2-submit'.")
        return 1
    print(f"\n{passed}/{seen} submission(s) correct." + ("" if keep else " Checked submissions were removed."))
    return 0 if passed else 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--keep", action="store_true", help="do not delete submissions after checking")
    ap.add_argument("--verify", nargs=3, metavar=("UVAID", "PLATFORM", "RECEIPT"), help="verify a receipt code")
    args = ap.parse_args()

    if args.verify:
        uvaid, platform, receipt = args.verify[0].lower(), args.verify[1].lower(), args.verify[2].upper()
        expected = _dp2.receipt_for(uvaid, _dp2.phrase_for(uvaid), platform)
        print("VALID receipt" if receipt == expected else "INVALID receipt")
        sys.exit(0 if receipt == expected else 1)
    sys.exit(drain(args.keep))


if __name__ == "__main__":
    main()
