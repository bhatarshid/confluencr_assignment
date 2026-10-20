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
> - **Phase 4 (Upcoming):** Cloud deployment (Render) and public repository finalization.

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

## 11. Scope & Phase 4 Roadmap

### Implemented in Phases 1, 2, and 3
- [x] Application foundation and configuration management
- [x] Connection-pooled SQLAlchemy 2.x database layer
- [x] Liveness (`GET /`) and readiness (`GET /health/ready`)
- [x] Integration with Supabase PostgreSQL `confluencr_asgnmt` tables
- [x] Webhook ingestion endpoint (`POST /v1/webhooks/transactions`) returning HTTP 202 in < 500ms
- [x] Status lookup endpoint (`GET /v1/transactions/{transaction_id}`)
- [x] Database-level idempotency and 409 conflict handling
- [x] Durable background worker process (`app/worker.py`)
- [x] Row-level locking job queue (`FOR UPDATE SKIP LOCKED`)
- [x] Configurable 30-second processing delay simulation
- [x] Atomic status transitions (`PROCESSING` -> `PROCESSED` with `processed_at`)
- [x] Exponential backoff retries and stale-lock recovery
- [x] 38 automated tests passing with zero failures

### Planned for Phase 4
- Cloud deployment (Render API and Worker web service / background worker)
- Final public GitHub repository publishing and assignment submission