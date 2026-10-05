# Setup: running Project 2 on your laptop

You will have **four terminals** open while you work. Give each one a name so you never type in the wrong one:

| Terminal | Runs | Open it | Leave it running? |
|---|---|---|---|
| **T1 · Emulator** | `moto_server -p 9324` (your SQS queue) | first | Yes, while you work |
| **T2 · Scatter API** | `python kit/scatter_api.py` | second | Yes, while you work |
| **T3 · Work** | everything you type: your flow, dbt, checks, git | any time | It is your working terminal |
| **T4 · Prefect** | `prefect server start` (the dashboard) | before your first flow run | Yes, while you work |

In every terminal: go to the project folder and **activate the virtual environment first** (your prompt starts with `(.venv)`). T1, T2 and T4 are servers: once started they keep printing and do not give you the prompt back. That is correct; do your typing in T3.

## 1. Python environment (once, in T3)

Use **Python 3.12 or 3.13**. (Very new Python versions such as 3.14 may not yet be supported by dbt.)

macOS / Linux:
```bash
cd ds3022-data-project-2
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Windows (PowerShell):
```powershell
cd ds3022-data-project-2
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Open `.env` and set `UVA_ID` to **your** computing ID. Leave the other lines as they are.

To activate the virtual environment in a new terminal later: `source .venv/bin/activate` (macOS / Linux) or `.venv\Scripts\Activate.ps1` (Windows).

## 2. Start the SQS emulator (T1)

```bash
moto_server -p 9324
```

Leave it running. It keeps queues **in memory**: if you stop it, every queue and message disappears (unlike real SQS). Just start it again and run your flow; its POST to the scatter API creates the queues again.

> Port 9324 is used on purpose. Moto's default (5000) is taken by AirPlay on macOS.

**Alternative (Docker):** instead of Moto you can run ElasticMQ on the same port:
```bash
docker compose -f kit/docker-compose.elasticmq.yml up -d
```
Use one emulator or the other, not both. Your code does not change.

## 3. Start the scatter API (T2)

```bash
python kit/scatter_api.py
```

It prints the port, the emulator address and the delay mode. `DP2_FAST=1` in `.env` gives 5-60 s delays for development. For your final full-delay run set `DP2_FAST=0` (30-900 s, allow about 15 minutes).

The scatter API reads `.env` only when it starts: after changing `DP2_FAST` or `DP2_CHAOS`, press **Ctrl+C** in T2 and start it again.

## 4. Check everything (T3)

```bash
python kit/smoke_test.py
```

You are ready when the last line is **`Emulator OK`**. Keep T1 and T2 running afterwards: your flow needs both every time it runs.

## 5. Start the Prefect server (T4)

```bash
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
```

Open the dashboard at http://127.0.0.1:4200. Your flow runs appear there, with their logs; you need screenshots of them for your README.

## 6. Day-to-day commands (T3)

```bash
python prefect-flow.py                                 # run your pipeline
dbt build --project-dir dbt --profiles-dir dbt         # run dbt by hand (from the repo root)
python kit/check_submission.py                         # check what you submitted, get a receipt
```

Peek at the queue from Python at any time:
```python
import boto3; from dotenv import load_dotenv; load_dotenv()
sqs = boto3.client("sqs")
print(sqs.list_queues())
```

## 7. When you stop working

Press **Ctrl+C** in T4, then T2, then T1, and close them. In T3 type `deactivate`. Next time, start them again in the same order: T1, T2, T4, then work in T3.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Could not connect to the endpoint URL: http://127.0.0.1:9324` | T1 is not running `moto_server -p 9324`, or the venv is not active there. |
| `ConnectionError` / `Connection refused` on port 8000 | T2 is not running `python kit/scatter_api.py`. |
| `ReadTimeout ... port=8000 ... read timeout=30` (Windows) | Your `.env` or code still says `localhost`. Use `127.0.0.1` in `AWS_ENDPOINT_URL_SQS` and in `SCATTER_URL`, restart T2, run again. |
| `Address already in use` / port 9324, 8000 or 4200 busy | That server is already running in another window. Use that window, or close it with Ctrl+C. |
| `command not found: moto_server` or `No module named ...` | The virtual environment is not active in this terminal. Activate it and try again. |
| `NoCredentialsError` or `NoRegionError` | `.env` is missing or not loaded. Check `load_dotenv()` runs before `boto3.client("sqs")`. |
| `KeyError: 'UVA_ID'` | `.env` has no `UVA_ID` line, or you ran the command outside the project folder. |
| Run not in the dashboard | T4 is not running, or `PREFECT_API_URL` is not set. Do step 5, then run the flow again. |
| Messages have no `MessageAttributes` | Pass `MessageAttributeNames=["All"]` to `receive_message`. |
| `KeyError: 'Messages'` | Nothing was visible yet. Use `resp.get("Messages", [])`. |
| `Can't open a connection to same database file with a different configuration` | You opened DuckDB with `read_only=True` while dbt (in the same Python process) holds a connection. Open it normally. |
| dbt creates `dp2.duckdb` in the wrong folder | Run dbt from the repo root with `--project-dir dbt --profiles-dir dbt`. |
| `pip install` fails on dbt, or `import dbt` errors | Check `python --version`; recreate the venv with Python 3.12 or 3.13. |
| PowerShell will not run `Activate.ps1` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then try again. |
| Queues vanished after restarting the emulator | Expected: it starts empty. Run your flow again; its POST recreates them. |
