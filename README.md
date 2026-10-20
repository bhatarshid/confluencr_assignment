# Transaction Webhook Service

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python 3.12](https://img.shields.io/badge/Python-3.12+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supabase-336791.svg?logo=postgresql&logoColor=white)](https://supabase.com)
[![SQLAlchemy 2.x](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00.svg?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![Tests](https://img.shields.io/badge/Tests-27%20Passed-brightgreen.svg)](https://docs.pytest.org/)

Production-grade asynchronous transaction webhook ingestion service built for the **Confluencr / Walnut Folks Group Full Stack Engineer Assessment**.

---

## 1. Project Purpose & Assessment Context

The objective of this application is to serve as a high-reliability webhook ingestion service that receives financial transaction webhooks from external payment processors, acknowledges them immediately (`HTTP 202 Accepted` within 500ms), and enables asynchronous background processing while guaranteeing idempotency, persistence, and auditability.

### Overall Assessment Requirements
1. **`GET /`** — Health-check liveness response with dynamic UTC timestamp.
2. **`GET /health/ready`** — Downstream Supabase PostgreSQL readiness check.
3. **`POST /v1/webhooks/transactions`** — Ingest payment webhook payload, guarantee database-level idempotency, and return HTTP 202 within 500ms.
4. **`GET /v1/transactions/{transaction_id}`** — Retrieve transaction status and lifecycle audit.
5. **Idempotency** — Prevent duplicate transaction processing even under concurrent requests without overwriting original data.
6. **Persistence** — Supabase PostgreSQL storage in `confluencr_asgnmt` schema.
7. **Resilience** — Safe error handling without losing accepted transactions.

> **Current Implementation Status:**
> - **Phase 1 (Completed):** Modular FastAPI foundation, Pydantic Settings, connection-pooled database layer, liveness and readiness probes, and unit tests.
> - **Phase 2 (Completed):** Webhook ingestion (`POST /v1/webhooks/transactions`), status lookup (`GET /v1/transactions/{transaction_id}`), Pydantic validation, database-level uniqueness idempotency, conflict detection, and 27 comprehensive automated tests.
> - **Phase 3 (Upcoming):** Asynchronous background worker, durable queue processing, and the 30-second simulated external processing delay.

---

## 2. Technology Stack

- **Language & Runtime:** Python 3.12 (compatible with Python 3.11+)
- **API Framework:** [FastAPI](https://fastapi.tiangolo.com)
- **ASGI Server:** [Uvicorn](https://www.uvicorn.org/)
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
│   ├── config.py             # Centralized Pydantic Settings & secret masking
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
│       └── transaction_service.py # Ingestion, idempotency, and query service
├── migrations/
│   └── 001_phase2_schema_adjustment.sql # Non-destructive schema migration
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Pytest fixtures and per-request session isolation
│   ├── test_config.py        # Configuration validation & secret masking tests (8 tests)
│   ├── test_health.py        # Health, readiness, and dynamic timestamp tests (8 tests)
│   └── test_transactions.py  # Ingestion, validation, idempotency, lookup, perf (11 tests)
├── .env.example              # Template configuration with safe placeholders
├── .gitignore                # Git ignore rules for venv, secrets, and caches
├── pytest.ini                # Pytest configuration
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

---

## 4. Database Schema & Mapping

The service integrates directly with the existing Supabase PostgreSQL schema **`confluencr_asgnmt`**.

### Reused Tables

#### 1. `confluencr_asgnmt.transactions`
| Column | Type | Constraints | Purpose |
|---|---|---|---|
| `id` | `BIGINT` | `PRIMARY KEY (IDENTITY)` | Internal surrogate primary key |
| `transaction_id` | `VARCHAR(128)` | `UNIQUE, NOT NULL, INDEX` | External unique transaction reference |
| `source_account` | `VARCHAR(128)` | `NOT NULL` | Debited account identifier (e.g. `acc_user_789`) |
| `destination_account` | `VARCHAR(128)` | `NOT NULL` | Credited account identifier (e.g. `acc_merchant_456`) |
| `amount` | `NUMERIC(18, 4)` | `NOT NULL` | Monetary amount preserving exact decimal precision |
| `currency` | `VARCHAR(3)` | `NOT NULL` | 3-letter uppercase ISO currency code (e.g. `INR`) |
| `status` | `VARCHAR(32)` | `NOT NULL, DEFAULT 'PROCESSING'` | Lifecycle status (`PROCESSING` or `PROCESSED`) |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL, DEFAULT now()` | UTC timestamp when accepted |
| `processed_at` | `TIMESTAMPTZ` | `NULL` | Null until background processing finishes |

#### 2. `confluencr_asgnmt.processing_jobs`
| Column | Type | Constraints | Purpose |
|---|---|---|---|
| `id` | `BIGINT` | `PRIMARY KEY (IDENTITY)` | Internal job ID |
| `transaction_id` | `BIGINT` | `FOREIGN KEY (transactions.id), UNIQUE` | 1-to-1 link to transaction record |
| `status` | `VARCHAR(32)` | `DEFAULT 'PENDING'` | Queue state (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`) |
| `attempts` | `SMALLINT` | `DEFAULT 0` | Execution attempt counter |
| `available_at` | `TIMESTAMPTZ` | `NULL` | Scheduled execution window |
| `locked_at` | `TIMESTAMPTZ` | `NULL` | Worker execution lease timestamp |
| `last_error` | `TEXT` | `NULL` | Error details if failure occurs |
| `created_at` | `TIMESTAMPTZ` | `DEFAULT now()` | Job enqueue timestamp |

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
*(On Windows PowerShell: `Copy-Item .env.example .env`)*

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
```

---

## 7. Running the Application Locally

Start the development server:

**Using PowerShell:**
```powershell
.\.venv\Scripts\uvicorn.exe app.main:app --reload --host 0.0.0.0 --port 8000
```

**Using Git Bash / Linux / macOS:**
```bash
./.venv/Scripts/uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Once running:
- **Interactive OpenAPI Documentation:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc Documentation:** [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

## 8. API Endpoints & Usage Examples

### A. Health Liveness Probe (`GET /`)
Immediate process liveness probe. Never queries the database.
```bash
curl -X GET http://127.0.0.1:8000/
```
**Response (HTTP 200 OK):**
```json
{
  "status": "HEALTHY",
  "current_time": "2026-10-20T13:22:19Z"
}
```

### B. Readiness Probe (`GET /health/ready`)
Verifies downstream Supabase PostgreSQL connection health.
```bash
curl -X GET http://127.0.0.1:8000/health/ready
```
**Response (HTTP 200 OK):**
```json
{
  "status": "READY",
  "current_time": "2026-10-20T13:25:00Z",
  "database": {
    "status": "CONNECTED",
    "latency_ms": 14.8,
    "message": "Database connection healthy."
  }
}
```

### C. Webhook Ingestion (`POST /v1/webhooks/transactions`)
Ingests transaction webhook and guarantees acknowledgment in < 500ms.

```bash
curl -X POST http://127.0.0.1:8000/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR"
  }'
```

**Response (HTTP 202 Accepted):**
```json
{
  "status": "ACCEPTED",
  "message": "Transaction accepted for asynchronous processing",
  "transaction_id": "txn_abc123def456"
}
```

### D. Transaction Status Lookup (`GET /v1/transactions/{transaction_id}`)
Retrieves persisted transaction state, timestamps, and details.

```bash
curl -X GET http://127.0.0.1:8000/v1/transactions/txn_abc123def456
```

**Response (HTTP 200 OK):**
```json
{
  "transaction_id": "txn_abc123def456",
  "source_account": "acc_user_789",
  "destination_account": "acc_merchant_456",
  "amount": 1500,
  "currency": "INR",
  "status": "PROCESSING",
  "created_at": "2026-10-20T13:30:00Z",
  "processed_at": null
}
```

**Unknown Transaction ID (HTTP 404 Not Found):**
```json
{
  "error": "NotFound",
  "message": "Transaction 'txn_unknown' was not found."
}
```

---

## 9. Idempotency & Conflict Handling

1. **Database Uniqueness Authority:**
   - Idempotency is enforced by the database unique constraint `uq_transactions_transaction_id`.
   - The application does not rely on vulnerable `SELECT`-then-`INSERT` sequences.
2. **Identical Retries:**
   - If the payment gateway retries a webhook with identical details, the application catches the uniqueness violation, rolls back cleanly, recognizes the identical record, and returns `HTTP 202 Accepted` with `"message": "Transaction already accepted and currently processing"`.
   - The original transaction timestamps and details are **never** overwritten.
3. **Conflicting Duplicate Payloads:**
   - If a duplicate `transaction_id` arrives with conflicting amounts, accounts, or currency, it is rejected with **`HTTP 409 Conflict`**:
     ```json
     {
       "error": "ConflictingTransaction",
       "message": "Transaction ID 'txn_abc123def456' already exists with conflicting transaction details."
     }
     ```
4. **Concurrent Requests:**
   - Under simultaneous concurrent requests, the database guarantees that exactly one transaction record and one processing job are created.

---

## 10. Automated Tests

Run the full automated test suite using pytest:
```bash
pytest -v
```

### Test Coverage Summary (27 Tests)
- **Configuration Tests (`tests/test_config.py` - 8 tests):**
  - Settings defaults, environment validation, log level validation, DB URL scheme checks, password masking, DB connectivity unconfigured/success/failure masking.
- **Health & Readiness Tests (`tests/test_health.py` - 8 tests):**
  - App startup, health 200 OK, dynamic ISO 8601 UTC timestamp validation, liveness independence from database, readiness probe healthy and degraded states.
- **Transaction API Tests (`tests/test_transactions.py` - 11 tests):**
  - Valid webhook returns 202 and persists `PROCESSING` status.
  - Validation: rejects missing fields, negative amounts, non-finite amounts, invalid currencies.
  - Idempotency: repeated identical requests return 202 without creating duplicate records.
  - Conflict detection: duplicate transaction ID with differing payload returns 409 Conflict without overwriting.
  - Concurrency: simultaneous duplicate requests produce exactly one persisted record.
  - Status retrieval: returns 200 with all fields, null `processed_at`, and returns 404 for unknown IDs.
  - Performance: verifies webhook endpoint response time targets < 500ms.

---

## 11. Security Guidance

- **No Hardcoded Secrets:** Credentials, database passwords, and API keys are loaded via environment variables and excluded from Git.
- **Secret Masking:** Database connection strings are masked via `sanitized_database_url` and Pydantic `SecretStr`.
- **Safe Error Responses:** Unhandled server exceptions return generic error responses without exposing tracebacks or connection credentials.

---

## 12. Scope & Phase 3 Roadmap

### Implemented in Phase 1 & Phase 2
- [x] Application foundation and configuration management
- [x] Connection-pooled SQLAlchemy 2.x database layer
- [x] Liveness (`GET /`) and readiness (`GET /health/ready`)
- [x] Integration with Supabase PostgreSQL `confluencr_asgnmt` tables
- [x] Webhook ingestion endpoint (`POST /v1/webhooks/transactions`)
- [x] Fast response target (< 500ms)
- [x] Status lookup endpoint (`GET /v1/transactions/{transaction_id}`)
- [x] Database-level idempotency and 409 conflict handling
- [x] 27 automated tests passing with zero failures

### Planned for Phase 3
- Asynchronous durable background worker
- 30-second simulated external processing delay
- Transition of transaction status to `PROCESSED` with `processed_at` timestamp
- Job retry policy, worker recovery, and dead-letter handling
- Cloud deployment (Render)