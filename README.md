# Transaction Webhook Service

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python 3.12](https://img.shields.io/badge/Python-3.12+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supabase-336791.svg?logo=postgresql&logoColor=white)](https://supabase.com)
[![SQLAlchemy 2.x](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00.svg?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![Tests](https://img.shields.io/badge/Tests-38%20Passed-brightgreen.svg)](https://docs.pytest.org/)

Production-grade asynchronous transaction webhook ingestion service and durable background worker built for the **Confluencr / Walnut Folks Group Full Stack Engineer Assessment**.

---

## 1. Project Purpose & Assessment Context

The objective of this application is to serve as a high-reliability webhook ingestion and background processing service that receives financial transaction webhooks from external payment processors, acknowledges them immediately (`HTTP 202 Accepted` within 500ms), and processes them asynchronously via a dedicated background worker simulating external processing (30-second delay) while guaranteeing idempotency, persistence, retries, and auditability.

### Overall Assessment Requirements
1. **`GET /`** — Health-check liveness response with dynamic UTC timestamp.
2. **`GET /health/ready`** — Downstream Supabase PostgreSQL readiness check.
3. **`POST /v1/webhooks/transactions`** — Ingest payment webhook payload, guarantee database-level idempotency, and return HTTP 202 within 500ms.
4. **`GET /v1/transactions/{transaction_id}`** — Retrieve transaction status and lifecycle audit (`PROCESSING` -> `PROCESSED`).
5. **Durable Asynchronous Worker** — Dedicated background worker process polling `confluencr_asgnmt.processing_jobs` using PostgreSQL `FOR UPDATE SKIP LOCKED`.
6. **30-Second Simulated Processing Delay** — Background delay simulating external banking/processor API latency, executed outside the webhook request.
7. **Idempotency & Resilience** — Prevent duplicate transactions and jobs under concurrent requests; exponential backoff retries and stale-lock recovery.
8. **Persistence** — Supabase PostgreSQL storage in `confluencr_asgnmt` schema.

> **Current Implementation Status:**
> - **Phase 1 (Completed):** Modular FastAPI foundation, Pydantic Settings, connection-pooled database layer, liveness and readiness probes, and unit tests.
> - **Phase 2 (Completed):** Webhook ingestion (`POST /v1/webhooks/transactions`), status lookup (`GET /v1/transactions/{transaction_id}`), Pydantic validation, database-level uniqueness idempotency, conflict detection, and 27 automated tests.
> - **Phase 3 (Completed):** Durable background worker (`app/worker.py`), atomic job claiming with row-level locking, 30-second processing delay simulation, status transition to `PROCESSED`, exponential backoff retries, stale-lock recovery, and 38 passing automated tests.
> - **Phase 4 (Completed):** Cloud deployment architecture (Render Blueprint & Dockerfile), process independence, worker platform health ping listener, secret safety audit, end-to-end verification, and public repository submission documentation.

---

## 2. Technology Stack

- **Language & Runtime:** Python 3.12 (compatible with Python 3.11+)
- **API Framework:** [FastAPI](https://fastapi.tiangolo.com)
- **ASGI Server:** [Uvicorn](https://www.uvicorn.org/)
- **Background Worker:** Standalone Python process with `asyncio` event loop and graceful shutdown
- **Configuration & Validation:** [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) & Pydantic v2
- **Database ORM & Pooling:** [SQLAlchemy 2.x](https://www.sqlalchemy.org/) with [psycopg3](https://www.psycopg.org/psycopg3/)
- **Target Database:** Supabase-hosted PostgreSQL (`confluencr_asgnmt` schema)
- **Testing:** [pytest](https://docs.pytest.org/), HTTPX TestClient, concurrent execution tests

---

## 3. Project Structure

```text
confluencr_assignment/
├── app/
│   ├── __init__.py           # Package marker
│   ├── main.py               # FastAPI entry point & lifespan lifecycle
│   ├── worker.py             # Dedicated background worker entry point (CLI runnable)
│   ├── config.py             # Centralized Pydantic Settings & worker configuration
│   ├── logging_config.py     # Clean standard logging configuration
│   ├── database.py           # SQLAlchemy engine, sessionmaker & connectivity checks
│   ├── api/
│   │   ├── __init__.py
│   │   ├── health.py         # Liveness (GET /) and Readiness (GET /health/ready)
│   │   ├── transactions.py   # Transaction status lookup (GET /v1/transactions/{id})
│   │   └── webhooks.py       # Webhook ingestion (POST /v1/webhooks/transactions)
│   ├── models/
│   │   ├── __init__.py
│   │   └── transaction.py    # SQLAlchemy models for confluencr_asgnmt tables
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── transaction.py    # Pydantic request & response validation schemas
│   └── services/
│       ├── __init__.py
│       ├── transaction_service.py # Ingestion, idempotency, and query service
│       └── worker_service.py      # Job claiming, 30s delay, completion, and retries
├── migrations/
│   ├── 001_phase2_schema_adjustment.sql # Non-destructive schema migration
│   └── 002_phase3_worker_indexes.sql    # Composite index for worker queue queries
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Pytest fixtures and per-request session isolation
│   ├── test_config.py        # Configuration validation & secret masking tests (8 tests)
│   ├── test_health.py        # Health, readiness, and dynamic timestamp tests (8 tests)
│   ├── test_transactions.py  # Ingestion, validation, idempotency, lookup, perf (11 tests)
│   └── test_worker.py        # Claiming, 30s delay, completion, retries, stale locks (11 tests)
├── .env.example              # Template configuration with safe placeholders
├── .gitignore                # Git ignore rules for venv, secrets, and caches
├── pytest.ini                # Pytest configuration
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

---

## 4. Background Worker Architecture & Job Lifecycle

The background processing system uses the existing PostgreSQL `processing_jobs` table as a durable message queue without introducing external brokers (Redis, RabbitMQ, Celery).

### A. Lifecycle State Machine

```text
               [ Webhook Received ]
                        |
                        v
     +-------------------------------------+
     | Job: PENDING                        |
     | Transaction: PROCESSING             |
     +-------------------------------------+
                        |
            (Worker claims job via
         FOR UPDATE SKIP LOCKED;
         commits claim, releases lock)
                        |
                        v
     +-------------------------------------+
     | Job: PROCESSING (locked_at set)     |
     | Transaction: PROCESSING             |
     +-------------------------------------+
                        |
         (Simulated 30s external delay)
                        |
        +---------------+---------------+
        |                               |
    [ Success ]                     [ Failure ]
        |                               |
        v                               v
+------------------------+      +-------------------------------+
| Job: COMPLETED         |      | attempts < MAX_ATTEMPTS:      |
| Transaction: PROCESSED |      |   Job: PENDING                |
| processed_at: <now>    |      |   available_at: <exponential> |
+------------------------+      +-------------------------------+
                                        |
                                [ attempts >= MAX_ATTEMPTS ]
                                        |
                                        v
                                +-------------------------------+
                                | Job: FAILED                   |
                                | Transaction: PROCESSING       |
                                | last_error: <sanitized error> |
                                +-------------------------------+
```

### B. Safe Multi-Worker Claiming
- Workers query eligible jobs where `status = 'PENDING'` and (`available_at <= now` or `available_at IS NULL`).
- Uses PostgreSQL **`FOR UPDATE SKIP LOCKED`** so multiple worker processes run concurrently without deadlocks or claiming the same row twice.
- The claim transaction updates `status = 'PROCESSING'`, `locked_at = now`, increments `attempts`, and **commits immediately**.
- The 30-second delay occurs **outside** any database lock or open transaction.

### C. Retries & Stale-Lock Recovery
- **Exponential Backoff:** If a failure occurs during processing, the job is reset to `PENDING` with `available_at = now + (base_seconds * 2^(attempts-1))`.
- **Max Attempts:** Exhausted attempts mark the job `FAILED` and record the sanitized error in `last_error`. The transaction remains in `PROCESSING` (never falsely marked `PROCESSED`).
- **Stale Lock Recovery:** If a worker crashes or freezes while holding a job, any job in `PROCESSING` with `locked_at < (now - WORKER_STALE_LOCK_SECONDS)` is reclaimed automatically on the next polling cycle.

---

## 5. Local Setup & Installation

### Step 1: Clone the repository
```bash
git clone https://github.com/bhatarshid/confluencr_assignment.git
cd confluencr_assignment
```

### Step 2: Create and activate virtual environment

**On Windows (PowerShell):**
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**On Windows (Git Bash):**
```bash
py -3.12 -m venv .venv
source .venv/Scripts/activate
```

**On Linux / macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3: Install dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 6. Environment Configuration

Copy the example environment configuration:
```bash
cp .env.example .env
```

Configure your `.env` variables:

```ini
APP_NAME="Transaction Webhook Service"
APP_VERSION="0.1.0"
APP_ENV="development"
LOG_LEVEL="INFO"
HOST="0.0.0.0"
PORT=8000

# Supabase PostgreSQL Connection
DB_SCHEMA="confluencr_asgnmt"
DATABASE_URL="postgresql+psycopg://postgres:[YOUR-PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres?sslmode=require"

# Pool Settings
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=1800

# Background Worker Settings
WORKER_POLL_INTERVAL_SECONDS=2.0
WORKER_PROCESSING_DELAY_SECONDS=30.0
WORKER_MAX_ATTEMPTS=3
WORKER_RETRY_BASE_SECONDS=5.0
WORKER_STALE_LOCK_SECONDS=120.0
WORKER_BATCH_SIZE=1
```

---

## 7. Running API & Worker as Separate Processes

To test the full asynchronous architecture locally, run the API server and the background worker in separate terminal windows.

### Terminal 1: Start the FastAPI API Server
```powershell
.\.venv\Scripts\uvicorn.exe app.main:app --reload --host 0.0.0.0 --port 8000
```
*(In Git Bash / Linux: `./.venv/Scripts/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`)*

### Terminal 2: Start the Background Worker
```powershell
.\.venv\Scripts\python.exe -m app.worker
```
*(In Git Bash / Linux: `./.venv/Scripts/python -m app.worker`)*

The worker will log:
```text
============================================================
Transaction Worker Started
  Poll Interval:     2.0s
  Processing Delay:  30.0s
  Max Attempts:      3
  Stale Lock Timeout: 120.0s
  Target Database:   postgresql+psycopg://postgres:***@db.../postgres
============================================================
```

---

## 8. End-to-End Workflow & Verification

### Step 1: Send Webhook
```bash
curl -X POST http://127.0.0.1:8000/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_demo_789",
    "source_account": "acc_user_111",
    "destination_account": "acc_merchant_222",
    "amount": 1500,
    "currency": "INR"
  }'
```
**Response (HTTP 202 Accepted within < 50ms):**
```json
{
  "status": "ACCEPTED",
  "message": "Transaction accepted for asynchronous processing",
  "transaction_id": "txn_demo_789"
}
```

### Step 2: Query Status Immediately (< 30 seconds)
```bash
curl -X GET http://127.0.0.1:8000/v1/transactions/txn_demo_789
```
**Response (HTTP 200 OK — currently processing):**
```json
{
  "transaction_id": "txn_demo_789",
  "source_account": "acc_user_111",
  "destination_account": "acc_merchant_222",
  "amount": 1500,
  "currency": "INR",
  "status": "PROCESSING",
  "created_at": "2026-10-20T14:10:00Z",
  "processed_at": null
}
```

### Step 3: Worker Log during 30s processing
```text
[INFO] worker_service: Claimed 1 job(s): [1]
[INFO] worker_service: Starting processing for job ID 1 (simulated external delay: 30.0s)
... (30 seconds pass) ...
[INFO] worker_service: Job ID 1 successfully completed. Transaction 'txn_demo_789' marked PROCESSED at 2026-10-20T14:10:30Z
[INFO] worker_service: Job ID 1 completed in 30.02s
```

### Step 4: Query Status After 30 Seconds
```bash
curl -X GET http://127.0.0.1:8000/v1/transactions/txn_demo_789
```
**Response (HTTP 200 OK — processed):**
```json
{
  "transaction_id": "txn_demo_789",
  "source_account": "acc_user_111",
  "destination_account": "acc_merchant_222",
  "amount": 1500,
  "currency": "INR",
  "status": "PROCESSED",
  "created_at": "2026-10-20T14:10:00Z",
  "processed_at": "2026-10-20T14:10:30Z"
}
```

---

## 9. Running Automated Tests

Run the full automated test suite using pytest:
```bash
pytest -v
```

### Test Coverage Summary (38 Tests Passing)
- **Configuration Tests (`tests/test_config.py` - 8 tests):**
  - Settings defaults, environment validation, log level validation, DB URL scheme checks, password masking, DB connectivity unconfigured/success/failure masking.
- **Health & Readiness Tests (`tests/test_health.py` - 8 tests):**
  - App startup, health 200 OK, dynamic ISO 8601 UTC timestamp validation, liveness independence from database, readiness probe healthy and degraded states.
- **Transaction API Tests (`tests/test_transactions.py` - 11 tests):**
  - Webhook validation (positive amounts, valid currencies, required fields), 202 Accepted response in < 500ms, idempotency (identical duplicate produces no extra records; conflicting duplicate returns 409 Conflict), concurrent requests safety, status lookup (200 OK or 404 Not Found).
- **Background Worker Tests (`tests/test_worker.py` - 11 tests):**
  - Pending job claiming, future scheduled jobs not claimed early, already processing jobs skipped, completed jobs skipped, status transition to `PROCESSED`, `processed_at` populated after completion, lock released, exponential backoff retries, max attempts leading to `FAILED`, stale-lock recovery, duplicate redelivery not requeuing completed jobs, and end-to-end status progression.

> **Note on Testing Performance:** The automated unit tests override `WORKER_PROCESSING_DELAY_SECONDS` with a short interval (0.05s) to guarantee fast, deterministic test execution without 30-second delays.

---

## 10. Security Guidance

- **No Hardcoded Secrets:** Credentials, database passwords, and API keys are loaded via environment variables and excluded from Git.
- **Secret Masking:** Database connection strings are masked via `sanitized_database_url` and Pydantic `SecretStr`.
- **Safe Error Responses:** Unhandled server exceptions and worker errors sanitize error strings before storage in `last_error` or returning to clients.

---

## 11. Cloud Deployment Architecture (Render & Containers)

The application is engineered to run as two decoupled, independently scalable processes operating against the same Supabase PostgreSQL database:

```text
       ┌────────────────────────────────────────────────────────┐
       │                External Payment Gateway                │
       └───────────────────────────┬────────────────────────────┘
                                   │ HTTP POST (webhook)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Render Cloud Platform                             │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Service 1: transaction-webhook-api (Web Service)                    │   │
│   │ - Entry Point: uvicorn app.main:app --host 0.0.0.0 --port $PORT     │   │
│   │ - Responsibilities: Webhook ingestion, 202 Accepted, Status lookup │   │
│   │ - Health Check: GET / (Liveness) & GET /health/ready (Readiness)    │   │
│   └──────────────────────────────────┬──────────────────────────────────┘   │
│                                      │ Inserts Job (PENDING)                │
│                                      ▼                                      │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Managed Database: Supabase PostgreSQL (confluencr_asgnmt schema)    │   │
│   │ - Table: transactions                                               │   │
│   │ - Table: processing_jobs (row-level locked queue)                   │   │
│   └──────────────────────────────────▲──────────────────────────────────┘   │
│                                      │ Claims Job (FOR UPDATE SKIP LOCKED)  │
│   ┌──────────────────────────────────┴──────────────────────────────────┐   │
│   │ Service 2: transaction-webhook-worker (Web Service / Worker)        │   │
│   │ - Entry Point: python -m app.worker                                 │   │
│   │ - Responsibilities: 30s processing delay, state transition          │   │
│   │ - Platform Probe: Lightweight HTTP responder on $PORT (0.0.0.0)     │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Hosting Provider Selection: Render ($0/month Free Tier)
- **Why Render?**
  - **Decoupled Services:** Allows deploying the API and the Worker as separate git-connected services.
  - **Zero Cost:** Native Render Background Workers require a paid Starter plan ($7/mo). However, our worker process includes a lightweight internal HTTP responder (`start_worker_health_server` listening on `$PORT`) returning `{"status":"HEALTHY","role":"worker"}`. This allows the worker to be deployed as a **Render Free Web Service** at **$0/month**.
  - **Automatic Deployments:** Deploys automatically on git push to `main`.
  - **Managed Secrets:** Database credentials are securely injected via environment variables without touching repository code.

### Deployment Method A: Render Blueprint (Infrastructure-as-Code)
A complete [render.yaml](render.yaml) specification is included in the root directory:
1. Connect your GitHub repository to [Render](https://render.com).
2. Go to **Blueprints** > **New Blueprint Instance**.
3. Select this repository; Render will automatically detect `render.yaml` and configure both services:
   - `transaction-webhook-api`
   - `transaction-webhook-worker`
4. Set the `DATABASE_URL` environment variable in the Render Dashboard for both services using your Supabase transaction pooler or direct connection string:
   ```text
   postgresql+psycopg://postgres.<project-ref>:[PASSWORD]@aws-0-[region].pooler.supabase.com:6543/postgres?sslmode=require
   ```
5. Click **Apply**. Both services will build and launch automatically.

### Deployment Method B: Containerized Deployment (Docker)
A production-ready [Dockerfile](Dockerfile) and [Procfile](Procfile) are provided for containerized environments:
```bash
# Build the unified container
docker build -t transaction-service:latest .

# Run Webhook API
docker run -d --name api -p 8000:8000 --env-file .env transaction-service:latest

# Run Background Worker
docker run -d --name worker --env-file .env transaction-service:latest python -m app.worker
```

### Production Free-Tier Considerations & Limitations
- **Inactivity Spin-Down:** On Render's Free tier, Web Services spin down after 15 minutes of inactivity. When a new webhook arrives, an initial cold start latency (~50s) may occur while the container boots.
- **Worker Availability on Free Tier:** If deploying the worker as a free web service, an external uptime ping (e.g. UptimeRobot or Cron-job.org) calling `https://transaction-webhook-worker.onrender.com/` every 10 minutes keeps the worker active and claiming jobs without interruption.
- **PostgreSQL Pool Limits:** Supabase free tier enforces a connection limit. Our database engine configures `pool_size=5`, `max_overflow=5`, and `pool_pre_ping=True` to prevent connection exhaustion.

---

## 12. Public End-to-End Verification Runbook

Follow this runbook to verify the deployed service or a locally running multi-process staging instance:

### Step 1: Health & Readiness
```bash
# Liveness
curl -i -X GET https://<your-api-domain>/

# Readiness (Supabase Connectivity & Latency)
curl -i -X GET https://<your-api-domain>/health/ready
```
*Expected: HTTP 200 OK with valid ISO-8601 UTC timestamp and database connected status.*

### Step 2: Submit Unique Webhook
```bash
curl -i -X POST https://<your-api-domain>/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "phase4-e2e-live-001",
    "source_account": "acc_sender_100",
    "destination_account": "acc_receiver_200",
    "amount": 250.75,
    "currency": "USD"
  }'
```
*Expected: HTTP 202 Accepted returned in < 500ms with `{"status": "ACCEPTED", "transaction_id": "phase4-e2e-live-001"}`.*

### Step 3: Immediate Status Inspection (Simulated 30s Delay Active)
```bash
curl -i -X GET https://<your-api-domain>/v1/transactions/phase4-e2e-live-001
```
*Expected: HTTP 200 OK with `"status": "PROCESSING"` and `"processed_at": null`.*

### Step 4: Status Inspection After 30+ Seconds
```bash
curl -i -X GET https://<your-api-domain>/v1/transactions/phase4-e2e-live-001
```
*Expected: HTTP 200 OK with `"status": "PROCESSED"` and populated `"processed_at"` timestamp.*

### Step 5: Verify Idempotency & Conflict Safety
```bash
# Redelivery of identical payload
curl -i -X POST https://<your-api-domain>/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "phase4-e2e-live-001",
    "source_account": "acc_sender_100",
    "destination_account": "acc_receiver_200",
    "amount": 250.75,
    "currency": "USD"
  }'
# Expected: HTTP 202 Accepted (idempotent duplicate response; no second job created)

# Conflicting payload with existing transaction_id
curl -i -X POST https://<your-api-domain>/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "phase4-e2e-live-001",
    "source_account": "acc_sender_100",
    "destination_account": "acc_receiver_DIFFERENT",
    "amount": 999.99,
    "currency": "USD"
  }'
# Expected: HTTP 409 Conflict with detail explaining payload mismatch
```

---

## 13. Summary of Assessment Deliverables

| Requirement | Implementation Artifact | Status |
| :--- | :--- | :--- |
| **Liveness & Readiness APIs** | [app/api/health.py](app/api/health.py) | Verified (Dynamic UTC time, DB ping) |
| **Webhook Ingestion (< 500ms)** | [app/api/webhooks.py](app/api/webhooks.py) | Verified (202 Accepted, non-blocking) |
| **Transaction Status API** | [app/api/transactions.py](app/api/transactions.py) | Verified (200 OK / 404 Not Found) |
| **Durable PostgreSQL Job Queue** | [app/services/worker_service.py](app/services/worker_service.py) | Verified (`FOR UPDATE SKIP LOCKED`) |
| **30-Second Processing Simulation** | `WORKER_PROCESSING_DELAY_SECONDS=30` | Verified (Cancellation-aware delay) |
| **Exponential Backoff & Stale Recovery**| `recover_stale_jobs`, retry backoff | Verified (Deterministic recovery) |
| **Automated Test Suite** | 38 tests across 4 test modules | **100% Passed (38 / 38)** |
| **Cloud Deployment Specification** | [render.yaml](render.yaml), [Dockerfile](Dockerfile) | Ready for Render / Containers |
| **Zero Secrets Committed** | Masked credentials, `.gitignore`, `.env.example` | Verified by audit |