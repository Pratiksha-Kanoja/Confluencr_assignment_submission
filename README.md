# Transaction Webhook Service

A Python (FastAPI) service that receives transaction webhooks from payment processors, acknowledges them immediately with `202 Accepted`, and processes them reliably in the background (with a simulated 30s external call), storing results in Postgres.

**Live API:** `<add deployed URL here>`

## Endpoints

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/` | Health check: `{"status": "HEALTHY", "current_time": "..."}` |
| `POST` | `/v1/webhooks/transactions` | Accept a transaction webhook, returns `202` |
| `GET` | `/v1/transactions/{transaction_id}` | Transaction status (array with one item), `404` if unknown |

Interactive docs are at `/docs`.

Example webhook:

```bash
curl -X POST "$BASE_URL/v1/webhooks/transactions" \
  -H "Content-Type: application/json" \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
# 202 {"status":"ACCEPTED","transaction_id":"txn_abc123def456","duplicate":false}
```

Status while processing, then ~30s later:

```json
[{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456",
  "amount":1500.0,"currency":"INR","status":"PROCESSED",
  "created_at":"2026-10-15T18:23:09Z","processed_at":"2026-10-15T18:23:39Z"}]
```

## Running locally

### With Docker (API + Postgres)

```bash
docker compose up --build
./scripts/smoke_test.sh http://localhost:8000
```

Use `PROCESSING_DELAY_SECONDS=5 docker compose up --build` for a faster feedback loop.

### Without Docker

Requires Python 3.9+. Uses a local SQLite file by default, or set `DATABASE_URL` (see `.env.example`).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
```

### Tests

```bash
pytest                                   # SQLite
docker compose up -d db
TEST_DATABASE_URL=postgresql://app:app@localhost:5432/transactions pytest   # real Postgres
```

The tests cover: single transaction processed after the delay, concurrent/repeated duplicates processed exactly once, webhook latency under a burst of 50 requests with concurrent processing, retry of transient failures, giving up after `MAX_ATTEMPTS`, recovery of in-flight work after a crash, and payload validation.

## Configuration

| Env var | Default | Purpose |
| ------- | ------- | ------- |
| `DATABASE_URL` | local SQLite | Postgres URL (`postgres://`, `postgresql://` and `?sslmode=require` are handled) |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external API latency |
| `LEASE_SECONDS` | `120` | How long a worker owns a transaction before it's considered abandoned |
| `MAX_ATTEMPTS` | `5` | Processing attempts before a transaction is marked `FAILED` |
| `RETRY_BACKOFF_SECONDS` | `5` | Base for exponential backoff between retries |
| `WORKER_CONCURRENCY` | `200` | Max transactions processed concurrently per instance |

## Deployment

Cloud database: **[Supabase](https://supabase.com)** (Postgres). App host: **[Render](https://render.com)** (Docker). The repo includes a `Dockerfile` and `render.yaml`.

### 1. Create the Supabase database

1. Sign up / log in at [supabase.com](https://supabase.com) and create a free project.
2. Open **Project Settings → Database**.
3. Under **Connection string**, choose **URI**, then **Session mode** (port `5432`).
4. Copy the URL and replace `[YOUR-PASSWORD]` with the database password you set at project creation.

Example shape:

```text
postgresql://postgres.xxxx:YOUR_PASSWORD@aws-0-ap-south-1.pooler.supabase.com:5432/postgres
```

### 2. Deploy the API on Render

1. Push this repo to GitHub.
2. On [Render](https://render.com) choose **New → Blueprint** (or **Web Service** from this repo).
3. Set env var `DATABASE_URL` to the Supabase URI from step 1.
4. Deploy. Health check is `/`; tables are created automatically on startup.

Then smoke-test:

```bash
./scripts/smoke_test.sh https://YOUR-SERVICE.onrender.com
```

Any container host (Railway, Fly.io, Cloud Run with min-instances ≥ 1) works the same way with the same `DATABASE_URL`. Serverless platforms that freeze the process after the response are not a good fit for the in-process worker.

## Technical choices

**FastAPI + async SQLAlchemy + asyncpg.** The workload is almost entirely I/O wait (DB calls and a 30s simulated external call), so a single async process can hold hundreds of in-flight transactions cheaply, and the webhook handler is never blocked by processing.

**Postgres as both the store and the queue.** The webhook handler does exactly one thing: an `INSERT ... ON CONFLICT (transaction_id) DO NOTHING` and then returns `202` (single-digit ms locally). Because the transaction is durably written *before* we acknowledge, nothing is lost if the process dies right after responding. If the insert fails, the endpoint returns `503` so the payment processor retries rather than us acking something we didn't store. Using the database as the queue avoids running and paying for a separate broker (Redis/SQS + Celery) while still giving durability — a reasonable trade-off at this scale.

**Idempotency at the database level.** `transaction_id` is the primary key, so duplicate webhooks — even concurrent ones hitting different instances — can't create a second row; the response just reports `"duplicate": true`. The final transition is a conditional `UPDATE ... WHERE status = 'PROCESSING'`, so even in the worst case a transaction is only ever marked processed once and `processed_at` is never overwritten.

**Background worker with leases.** An asyncio worker runs inside the app. It claims work with `SELECT ... FOR UPDATE SKIP LOCKED` and sets a `locked_until` lease, so multiple instances can run side by side without picking the same transaction. It's woken immediately when a new webhook arrives and also polls every few seconds as a safety net. If the process crashes or is redeployed mid-processing, the lease expires and the transaction is picked up again (`attempts` increments). Transient failures are retried with exponential backoff; after `MAX_ATTEMPTS` the transaction is marked `FAILED` with `last_error` recorded, instead of retrying forever.

**Amounts as `NUMERIC(18,2)`.** Money is stored as exact decimals, never floats; it's serialized as a JSON number in responses to match the spec.

### Known limitations / what I'd add next

- The simulated "external call" is a sleep. A real one should send `transaction_id` as an idempotency key, since a crash between the call and the DB update causes a retry (at-least-once execution, exactly-once recorded result).
- Webhook signature verification (e.g. Razorpay's `X-Razorpay-Signature` HMAC) and auth on the query endpoint.
- Alembic migrations instead of `create_all` on startup.
- On free tiers that sleep when idle, the first request after a cold start can exceed 500ms; an always-on instance avoids this.
