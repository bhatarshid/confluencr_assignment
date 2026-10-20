# Transaction Webhook Service

[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Python 3.12](https://img.shields.io/badge/Python-3.12+-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supabase-336791.svg?logo=postgresql&logoColor=white)](https://supabase.com)
[![SQLAlchemy 2.x](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00.svg?logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![Tests](https://img.shields.io/badge/Tests-pytest-blue.svg)](https://docs.pytest.org/)

Production-grade asynchronous transaction webhook ingestion service built for the **Confluencr / Walnut Folks Group Full Stack Engineer Assessment**.

---

## 1. Project Purpose & Assessment Context

The objective of this application is to serve as a high-reliability webhook ingestion service that receives financial transaction webhooks from external payment processors, acknowledges them immediately (`HTTP 202 Accepted` within 500ms), and processes them asynchronously against an external API simulation (30s delay) while guaranteeing idempotency, persistence, and auditability.

### Overall Assessment Requirements
1. **`GET /`** — Health-check liveness response with dynamic UTC timestamp.
2. **`POST /v1/webhooks/transactions`** — Accept payment webhook payload and enqueue for background execution.
3. **`GET /v1/transactions/{transaction_id}`** — Retrieve transaction status and lifecycle audit.
4. **Idempotency** — Prevent duplicate transaction processing even under concurrent requests.
5. **Persistence** — Supabase PostgreSQL storage for transactions and status updates.
6. **Resilience** — Safe error handling without losing accepted transactions.

> **Current Phase Status (Phase 1):**
> Phase 1 delivers the clean, robust, and tested **application foundation**:
> - Centralized Pydantic configuration and secret masking
> - Modular FastAPI application structure with predictable lifecycle
> - Connection-pooled SQLAlchemy 2.x database layer for Supabase PostgreSQL
> - High-performance dynamic health-check endpoint (`GET /`)
> - Explicit database readiness check (`GET /health/ready`)
> - Automated pytest test suite independent of network dependencies

---

## 2. Technology Stack

- **Language & Runtime:** Python 3.12 (compatible with Python 3.11+)
- **API Framework:** [FastAPI](https://fastapi.tiangolo.com)
- **ASGI Server:** [Uvicorn](https://www.uvicorn.org/)
- **Configuration & Validation:** [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
- **Database ORM & Pooling:** [SQLAlchemy 2.x](https://www.sqlalchemy.org/) with [psycopg3](https://www.psycopg.org/psycopg3/)
- **Target Database:** Supabase-hosted PostgreSQL
- **Testing:** [pytest](https://docs.pytest.org/), HTTPX TestClient

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
│   └── api/
│       ├── __init__.py
│       └── health.py         # Liveness (GET /) and Readiness (GET /health/ready)
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Pytest fixtures and TestClient configuration
│   ├── test_config.py        # Configuration validation & secret masking tests
│   └── test_health.py        # Health, readiness, and dynamic timestamp tests
├── .env.example              # Template configuration with safe placeholders
├── .gitignore                # Git ignore rules for venv, secrets, and caches
├── pytest.ini                # Pytest configuration
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

---

## 4. Prerequisites

- Python 3.11 or newer (Python 3.12 recommended)
- A Supabase project with PostgreSQL credentials (or local PostgreSQL instance)
- Git

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

| Variable | Default | Description |
|---|---|---|
| `APP_NAME` | `Transaction Webhook Service` | Application display name |
| `APP_VERSION` | `0.1.0` | Semantic version string |
| `APP_ENV` | `development` | Environment (`development`, `staging`, `production`, `test`) |
| `LOG_LEVEL` | `INFO` | Log severity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) |
| `DATABASE_URL` | *None* | PostgreSQL connection string for Supabase |
| `DB_POOL_SIZE` | `5` | Core connection pool size |
| `DB_MAX_OVERFLOW` | `10` | Max overflow connections |
| `DB_POOL_TIMEOUT`| `30` | Seconds to wait for connection from pool |
| `DB_POOL_RECYCLE`| `1800` | Connection recycling interval in seconds |

### Supabase PostgreSQL Connection Setup

Supabase provides two primary connection strings in **Project Settings -> Database**:

1. **Direct Connection (port 5432):**
   ```text
   DATABASE_URL="postgresql+psycopg://postgres:[YOUR-PASSWORD]@db.[PROJECT-REF].supabase.co:5432/postgres?sslmode=require"
   ```
2. **Supabase Connection Pooler (Transaction Mode, port 6543):**
   ```text
   DATABASE_URL="postgresql+psycopg://postgres.[PROJECT-REF]:[YOUR-PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres?sslmode=require"
   ```

> **Note on Supabase Pooler:** When connecting through the transaction pooler (port 6543), server-side prepared statements are automatically disabled by the database layer (`prepare_threshold = None`) to ensure full compatibility with PgBouncer.

---

## 7. Running the Application Locally

Start the local development server:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Once running:
- **Interactive OpenAPI Documentation:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **ReDoc Documentation:** [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)
- **Health Check Endpoint:** [http://127.0.0.1:8000/](http://127.0.0.1:8000/)
- **Readiness Check Endpoint:** [http://127.0.0.1:8000/health/ready](http://127.0.0.1:8000/health/ready)

---

## 8. Health Check Endpoints & Behavior

### Liveness Probe (`GET /`)
Designed for container orchestrators and load balancers to verify process responsiveness. It executes in sub-millisecond time and **never** makes database or network calls.

**Request:**
```http
GET / HTTP/1.1
Host: localhost:8000
```

**Response (HTTP 200 OK):**
```json
{
  "status": "HEALTHY",
  "current_time": "2026-10-20T13:22:19Z"
}
```

- Dynamic ISO 8601 UTC timestamp (`Z` suffix).
- Guaranteed fast response time.

### Readiness Probe (`GET /health/ready`)
Checks operational status of backing dependencies (Supabase PostgreSQL connectivity).

**Healthy Response (HTTP 200 OK):**
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

**Degraded / Disconnected Response (HTTP 503 Service Unavailable):**
```json
{
  "status": "NOT_READY",
  "current_time": "2026-10-20T13:25:00Z",
  "database": {
    "status": "UNCONFIGURED",
    "message": "DATABASE_URL is not configured."
  }
}
```

---

## 9. Running Automated Tests

Run the test suite using pytest:
```bash
pytest
```

Run tests with verbose output:
```bash
pytest -v
```

The default test suite:
- Executes 100% deterministically without requiring a live database connection or network.
- Tests application startup and health endpoint response.
- Verifies dynamic UTC ISO 8601 timestamps and non-hardcoded behavior.
- Tests configuration validation (invalid environment, invalid log level, unsupported database schemes).
- Verifies secret masking to prevent credentials from leaking in logs or string representations.
- Tests database connectivity logic and readiness endpoints under both success and failure states using mocks.

---

## 10. Security Guidance

- **No Hardcoded Secrets:** Credentials, database passwords, and API keys must never be committed to source control.
- **Git Protection:** `.env` and local environment files are ignored via `.gitignore`.
- **Secret Masking:** Database connection strings are masked via `sanitized_database_url` and Pydantic `SecretStr` before any logging.
- **Safe Error Responses:** Unhandled server exceptions return generic error responses without exposing tracebacks or connection credentials.

---

## 11. Current Scope & Deferred Features

### Implemented in Phase 1
- [x] Clean modular project structure
- [x] Application factory and lifespan startup/shutdown lifecycle
- [x] Centralized Pydantic Settings configuration
- [x] SQLAlchemy 2.x engine and connection pooling abstraction
- [x] Fast liveness endpoint (`GET /`) with dynamic UTC timestamp
- [x] Dependency-aware readiness endpoint (`GET /health/ready`)
- [x] Standard logging without secret exposure
- [x] Comprehensive pytest automated unit test suite (16 tests)
- [x] `.env.example`, `.gitignore`, `pytest.ini`, and complete `README.md`

### Intentionally Deferred to Phase 2 & Beyond
- Database migrations & transaction schema models
- `POST /v1/webhooks/transactions` ingestion endpoint
- `GET /v1/transactions/{transaction_id}` query endpoint
- Asynchronous background worker & 30-second processing simulation
- Transaction deduplication & idempotency handling
- Cloud deployment (Render)