from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.models import Transaction, TransactionStatus, utcnow
from app.schemas import TransactionWebhook


@dataclass(frozen=True)
class ClaimedTransaction:
    transaction_id: str
    attempt: int


async def insert_if_absent(session: AsyncSession, payload: TransactionWebhook) -> bool:
    """Persist a new transaction. Returns False if the transaction_id already exists."""
    insert = pg_insert if session.bind.dialect.name == "postgresql" else sqlite_insert
    stmt = (
        insert(Transaction)
        .values(
            transaction_id=payload.transaction_id,
            source_account=payload.source_account,
            destination_account=payload.destination_account,
            amount=payload.amount,
            currency=payload.currency,
            status=TransactionStatus.PROCESSING.value,
            created_at=utcnow(),
            attempts=0,
        )
        .on_conflict_do_nothing(index_elements=[Transaction.transaction_id])
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount == 1


async def get_transaction(session: AsyncSession, transaction_id: str) -> Optional[Transaction]:
    return await session.get(Transaction, transaction_id)


async def claim_next(
    factory: async_sessionmaker[AsyncSession], settings: Settings
) -> Optional[ClaimedTransaction]:
    """Atomically lease the oldest pending transaction.

    A lease (locked_until) rather than a long-held row lock means a crashed
    worker's transactions become claimable again once the lease expires.
    """
    while True:
        async with factory() as session, session.begin():
            now = utcnow()
            stmt = (
                select(Transaction)
                .where(
                    Transaction.status == TransactionStatus.PROCESSING.value,
                    or_(Transaction.locked_until.is_(None), Transaction.locked_until <= now),
                )
                .order_by(Transaction.created_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            txn = (await session.execute(stmt)).scalar_one_or_none()
            if txn is None:
                return None

            if txn.attempts >= settings.max_attempts:
                txn.status = TransactionStatus.FAILED.value
                txn.locked_until = None
                txn.last_error = txn.last_error or "max attempts exceeded"
                continue

            txn.attempts += 1
            txn.locked_until = now + timedelta(seconds=settings.lease_seconds)
            return ClaimedTransaction(transaction_id=txn.transaction_id, attempt=txn.attempts)


async def mark_processed(factory: async_sessionmaker[AsyncSession], transaction_id: str) -> bool:
    async with factory() as session, session.begin():
        result = await session.execute(
            update(Transaction)
            .where(
                Transaction.transaction_id == transaction_id,
                Transaction.status == TransactionStatus.PROCESSING.value,
            )
            .values(
                status=TransactionStatus.PROCESSED.value,
                processed_at=utcnow(),
                locked_until=None,
                last_error=None,
            )
        )
        return result.rowcount == 1


async def release_for_retry(
    factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    claimed: ClaimedTransaction,
    error: str,
) -> None:
    values = {"last_error": error[:2000]}
    if claimed.attempt >= settings.max_attempts:
        values.update(status=TransactionStatus.FAILED.value, locked_until=None)
    else:
        backoff = settings.retry_backoff_seconds * (2 ** (claimed.attempt - 1))
        values["locked_until"] = utcnow() + timedelta(seconds=backoff)

    async with factory() as session, session.begin():
        await session.execute(
            update(Transaction)
            .where(
                Transaction.transaction_id == claimed.transaction_id,
                Transaction.status == TransactionStatus.PROCESSING.value,
            )
            .values(**values)
        )
