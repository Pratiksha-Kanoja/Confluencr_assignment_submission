import asyncio
import logging
from typing import Awaitable, Callable, Optional, Set

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app import repository
from app.config import Settings
from app.repository import ClaimedTransaction

logger = logging.getLogger(__name__)

Processor = Callable[[ClaimedTransaction], Awaitable[None]]


class TransactionWorker:
    """Durable in-process worker.

    Pending work lives in the database, not in memory: the webhook handler only
    inserts a row and nudges the worker. The worker leases rows, runs the
    (simulated) processing concurrently up to ``worker_concurrency``, and writes
    the result back. Anything in flight during a crash/redeploy is picked up again
    once its lease expires.
    """

    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        processor: Optional[Processor] = None,
    ) -> None:
        self._factory = factory
        self._settings = settings
        self._processor = processor or self._simulate_external_call
        self._wake = asyncio.Event()
        self._slots = asyncio.Semaphore(settings.worker_concurrency)
        self._tasks: Set[asyncio.Task] = set()
        self._runner: Optional[asyncio.Task] = None

    def start(self) -> None:
        if self._runner is None:
            self._runner = asyncio.create_task(self._run(), name="transaction-worker")

    async def stop(self) -> None:
        if self._runner is None:
            return
        self._runner.cancel()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(self._runner, *self._tasks, return_exceptions=True)
        self._runner = None

    def notify(self) -> None:
        self._wake.set()

    async def _simulate_external_call(self, claimed: ClaimedTransaction) -> None:
        await asyncio.sleep(self._settings.processing_delay_seconds)

    async def _run(self) -> None:
        logger.info("Transaction worker started")
        while True:
            self._wake.clear()
            await self._dispatch_available()
            try:
                await asyncio.wait_for(
                    self._wake.wait(), timeout=self._settings.worker_poll_interval_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _dispatch_available(self) -> None:
        while True:
            await self._slots.acquire()
            try:
                claimed = await repository.claim_next(self._factory, self._settings)
            except asyncio.CancelledError:
                self._slots.release()
                raise
            except Exception:
                self._slots.release()
                logger.exception("Failed to claim transaction; will retry on next poll")
                return

            if claimed is None:
                self._slots.release()
                return

            task = asyncio.create_task(self._process(claimed))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _process(self, claimed: ClaimedTransaction) -> None:
        txn_id = claimed.transaction_id
        try:
            logger.info("Processing %s (attempt %d)", txn_id, claimed.attempt)
            await self._processor(claimed)
            if await repository.mark_processed(self._factory, txn_id):
                logger.info("Processed %s", txn_id)
            else:
                logger.warning("%s was already finalized elsewhere", txn_id)
        except asyncio.CancelledError:
            # Shutdown: leave the lease to expire so another instance/restart retries it.
            raise
        except Exception as exc:
            logger.exception("Processing %s failed", txn_id)
            try:
                await repository.release_for_retry(
                    self._factory, self._settings, claimed, f"{type(exc).__name__}: {exc}"
                )
            except Exception:
                logger.exception("Could not record failure for %s; lease will expire", txn_id)
        finally:
            self._slots.release()
            self.notify()
