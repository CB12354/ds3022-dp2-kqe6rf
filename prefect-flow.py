"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
import os
import time
import duckdb
from dbt.cli.main import dbtRunner 
import boto3
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS
@task
def prepare_landing() -> None:
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("create schema if not exists raw")
        con.execute("create or replace table raw.fragments ("
                    "order_no varchar, word varchar, "
                    "received_at timestamp)")
 
 
def stored_count(con) -> int:
    return con.execute("select count(distinct order_no) "
                       "from raw.fragments").fetchone()[0]

@task (retries=2, retry_delay_seconds=5)
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL."""
    logger = get_run_logger()
    # Make a POST request to send all the messages to the SQS queue.
    # The Scatter python file is running in the background, so making this POST request activates the script.
    resp = requests.post(SCATTER_URL, timeout=30)
    # Stop if the scatter didn't activate
    resp.raise_for_status()
    payload = resp.json()
    logger.info("Scatter API answered: %s", payload)
    # Get the queue location to pass to the monitoring step
    return payload["sqs_url"]

# Handy array to make these long names an index
COUNTERS = ["ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"]
 
def get_counts(queue_url: str) -> dict:
    """Helper function to get all of the message counts in each bucket."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=COUNTERS)["Attributes"]
    return {"visible":   int(attrs["ApproximateNumberOfMessages"]),
            "in_flight": int(attrs["ApproximateNumberOfMessagesNotVisible"]),
            "delayed":   int(attrs["ApproximateNumberOfMessagesDelayed"])}


@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        # Poll queue counters
        c = get_counts(queue_url)
        logger.info("visible=%d  in_flight=%d  delayed=%d",
                    c["visible"], c["in_flight"], c["delayed"])
        # Stop monitoring if all messages are visible
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c
        # Start cooldown if some messages are delayed
        time.sleep(poll_s)
    # If polling doesn't work fast enough, timeout and exit
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")

@task
def collect_messages(queue_url: str, expected: int = 21,
                     timeout_s: int = 1200) -> int:
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    with duckdb.connect(DUCKDB_PATH) as con:
        while time.monotonic() < deadline:
            stored, c = stored_count(con), get_counts(queue_url)
            logger.info("stored %d/%d | visible=%d in_flight=%d delayed=%d",
                        stored, expected, c["visible"],
                        c["in_flight"], c["delayed"])
            if stored >= expected and sum(c.values()) == 0:
                return stored
            resp = sqs.receive_message(QueueUrl=queue_url,
                                       MaxNumberOfMessages=10,
                                       MessageAttributeNames=["All"],
                                       WaitTimeSeconds=10)
            for msg in resp.get("Messages", []):
                a = msg["MessageAttributes"]
                con.execute("insert into raw.fragments "
                            "values (?, ?, current_timestamp)",
                            [a["order_no"]["StringValue"],
                             a["word"]["StringValue"]])
                sqs.delete_message(QueueUrl=queue_url,
                                   ReceiptHandle=msg["ReceiptHandle"])
        stored = stored_count(con)
        # test 1: delete row

        # test 2: duplicate row

    raise TimeoutError(f"Only {stored}/{expected} fragments "
                       f"after {timeout_s} s")


@task
def dbt_build() -> None:
    #duckdb.connect(DUCKDB_PATH).execute("insert into raw.fragments select order_no, word||'X', received_at from raw.fragments where order_no='7'")
    res = dbtRunner().invoke(["build", "--project-dir", "dbt",
                              "--profiles-dir", "dbt"])
    if not res.success:
        raise RuntimeError("dbt tests failed: not submitting")
    get_run_logger().info("dbt build passed: all tests green")
 
 
@task
def read_phrase() -> str:
    with duckdb.connect(DUCKDB_PATH) as con:
        n, phrase = con.execute("select fragment_count, phrase "
                                "from assembled_phrase").fetchone()
    get_run_logger().info("Assembled %d fragments: %s", n, phrase)
    return phrase


@task(retries=2, retry_delay_seconds=5)
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    resp = sqs.send_message(
        QueueUrl=url, MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}})
    status = resp["ResponseMetadata"]["HTTPStatusCode"]
    get_run_logger().info("Submitted, HTTP %s", status)
    if status != 200:
        raise RuntimeError(f"Submission returned HTTP {status}")
    return status


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline(timeout_s: int = 1200):
    queue_url = populate_queue()
    prepare_landing()
    collect_messages(queue_url, timeout_s=timeout_s)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)


if __name__ == "__main__":
    dp2_pipeline()
