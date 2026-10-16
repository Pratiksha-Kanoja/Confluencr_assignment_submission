import asyncio
import time
from datetime import timedelta

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app.db import create_engine, create_session_factory, init_db
from app.main import create_app
from app.models import Transaction, utcnow

from .conftest import PROCESSING_DELAY, RecordingProcessor, make_settings, wait_for_status, webhook


async def test_health(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "HEALTHY"
    assert body["current_time"].endswith("Z")


async def test_single_transaction_is_processed_after_delay(client, processor):
    resp = await client.post("/v1/webhooks/transactions", json=webhook())
    assert resp.status_code == 202
    assert resp.json() == {
        "status": "ACCEPTED",
        "transaction_id": "txn_abc123def456",
        "duplicate": False,
    }

    pending = (await client.get("/v1/transactions/txn_abc123def456")).json()
    assert pending[0]["status"] == "PROCESSING"
    assert pending[0]["processed_at"] is None

    done = await wait_for_status(client, "txn_abc123def456", "PROCESSED")
    assert done["amount"] == 1500
    assert done["currency"] == "INR"
    assert done["processed_at"] >= done["created_at"]
    assert len(processor.calls) == 1


async def test_duplicate_webhooks_processed_once(client, processor):
    responses = await asyncio.gather(
        *(client.post("/v1/webhooks/transactions", json=webhook("txn_dup")) for _ in range(10))
    )
    assert all(r.status_code == 202 for r in responses)
    assert sum(not r.json()["duplicate"] for r in responses) == 1

    await wait_for_status(client, "txn_dup", "PROCESSED")
    resp = await client.post("/v1/webhooks/transactions", json=webhook("txn_dup"))
    assert resp.status_code == 202
    assert resp.json()["duplicate"] is True

    await asyncio.sleep(PROCESSING_DELAY * 2)
    assert [c.transaction_id for c in processor.calls] == ["txn_dup"]


async def test_webhook_stays_fast_under_load(client, processor):
    n = 50
    latencies = []

    async def send(i: int) -> None:
        start = time.perf_counter()
        resp = await client.post("/v1/webhooks/transactions", json=webhook(f"txn_load_{i}"))
        latencies.append(time.perf_counter() - start)
        assert resp.status_code == 202

    await asyncio.gather(*(send(i) for i in range(n)))
    assert max(latencies) < 0.5

    start = time.monotonic()
    for i in range(n):
        await wait_for_status(client, f"txn_load_{i}", "PROCESSED")
    # Processed concurrently, not one after another.
    assert time.monotonic() - start < PROCESSING_DELAY * 10
    assert len(processor.calls) == n


@pytest.mark.parametrize("processor", [RecordingProcessor(fail_times=2)])
async def test_transient_failures_are_retried(client, processor):
    await client.post("/v1/webhooks/transactions", json=webhook("txn_retry"))
    await wait_for_status(client, "txn_retry", "PROCESSED")
    assert [c.attempt for c in processor.calls] == [1, 2, 3]


async def test_gives_up_after_max_attempts(tmp_path):
    settings = make_settings(tmp_path, max_attempts=2)
    processor = RecordingProcessor(delay=0.01, fail_times=100)
    app = create_app(settings, processor)
    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/v1/webhooks/transactions", json=webhook("txn_fail"))
            await wait_for_status(client, "txn_fail", "FAILED")
    assert len(processor.calls) == 2


async def test_in_flight_work_survives_restart(tmp_path):
    """A row leased by a worker that crashed is picked up after the lease expires."""
    settings = make_settings(tmp_path)
    engine = create_engine(settings)
    await init_db(engine)
    factory = create_session_factory(engine)
    async with factory() as session, session.begin():
        session.add(
            Transaction(
                transaction_id="txn_orphan",
                source_account="a",
                destination_account="b",
                amount=10,
                currency="USD",
                status="PROCESSING",
                created_at=utcnow() - timedelta(minutes=5),
                attempts=1,
                locked_until=utcnow() - timedelta(seconds=1),
            )
        )
    await engine.dispose()

    processor = RecordingProcessor()
    app = create_app(settings, processor)
    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await wait_for_status(client, "txn_orphan", "PROCESSED")
    assert processor.calls[0].attempt == 2


async def test_unknown_transaction_returns_404(client):
    resp = await client.get("/v1/transactions/does_not_exist")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": -5},
        {"amount": 0},
        {"currency": "RUPEES"},
        {"transaction_id": ""},
        {"source_account": None},
    ],
)
async def test_invalid_payload_rejected(client, overrides):
    resp = await client.post("/v1/webhooks/transactions", json=webhook(**overrides))
    assert resp.status_code == 422
