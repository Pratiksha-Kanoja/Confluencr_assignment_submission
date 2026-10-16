import asyncio
import os
import time
from typing import Any, Dict, List

import httpx
import pytest
from asgi_lifespan import LifespanManager

from app import models  # noqa: F401
from app.config import Settings
from app.db import Base, create_engine
from app.main import create_app
from app.repository import ClaimedTransaction

PROCESSING_DELAY = 0.3


TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


def make_settings(tmp_path, **overrides: Any) -> Settings:
    values: Dict[str, Any] = dict(
        database_url=TEST_DATABASE_URL or f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        processing_delay_seconds=PROCESSING_DELAY,
        lease_seconds=5,
        retry_backoff_seconds=0.05,
        worker_poll_interval_seconds=0.1,
    )
    values.update(overrides)
    return Settings(_env_file=None, **values)


class RecordingProcessor:
    """Simulated external call that records every invocation."""

    def __init__(self, delay: float = PROCESSING_DELAY, fail_times: int = 0) -> None:
        self.delay = delay
        self.fail_times = fail_times
        self.calls: List[ClaimedTransaction] = []

    async def __call__(self, claimed: ClaimedTransaction) -> None:
        self.calls.append(claimed)
        await asyncio.sleep(self.delay)
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("upstream unavailable")


@pytest.fixture(autouse=True)
async def _clean_shared_database(tmp_path):
    if not TEST_DATABASE_URL:
        return
    engine = create_engine(make_settings(tmp_path))
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
def processor() -> RecordingProcessor:
    return RecordingProcessor()


@pytest.fixture
def settings(tmp_path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
async def client(settings, processor):
    app = create_app(settings, processor)
    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


def webhook(txn_id: str = "txn_abc123def456", **overrides: Any) -> Dict[str, Any]:
    body = {
        "transaction_id": txn_id,
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
    body.update(overrides)
    return body


async def wait_for_status(
    client: httpx.AsyncClient, txn_id: str, status: str, timeout: float = 5.0
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        resp = await client.get(f"/v1/transactions/{txn_id}")
        if resp.status_code == 200 and resp.json()[0]["status"] == status:
            return resp.json()[0]
        if time.monotonic() > deadline:
            raise AssertionError(f"{txn_id} never reached {status}: {resp.text}")
        await asyncio.sleep(0.05)
