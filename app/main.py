import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import AsyncIterator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app import repository
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory, init_db
from app.schemas import HealthOut, TransactionOut, TransactionWebhook, WebhookAck, to_utc_iso
from app.worker import Processor, TransactionWorker

logger = logging.getLogger("app")


def create_app(settings: Optional[Settings] = None, processor: Optional[Processor] = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        await init_db(engine)
        factory = create_session_factory(engine)
        worker = TransactionWorker(factory, settings, processor)
        if settings.worker_enabled:
            worker.start()

        app.state.session_factory = factory
        app.state.worker = worker
        try:
            yield
        finally:
            await worker.stop()
            await engine.dispose()

    app = FastAPI(title="Transaction Webhook Service", version="1.0.0", lifespan=lifespan)

    async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
        async with request.app.state.session_factory() as session:
            yield session

    @app.get("/", response_model=HealthOut)
    async def health() -> HealthOut:
        return HealthOut(status="HEALTHY", current_time=to_utc_iso(datetime.now(timezone.utc)))

    @app.post(
        "/v1/webhooks/transactions",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=WebhookAck,
    )
    async def receive_transaction_webhook(
        payload: TransactionWebhook,
        request: Request,
        session: AsyncSession = Depends(get_session),
    ) -> WebhookAck:
        try:
            inserted = await repository.insert_if_absent(session, payload)
        except Exception:
            # Never ack what we failed to persist: a non-2xx makes the payment
            # processor retry, and the insert is idempotent so retries are safe.
            logger.exception("Failed to persist webhook %s", payload.transaction_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not accept webhook, please retry",
            )

        if inserted:
            request.app.state.worker.notify()
        else:
            logger.info("Duplicate webhook for %s ignored", payload.transaction_id)
        return WebhookAck(transaction_id=payload.transaction_id, duplicate=not inserted)

    @app.get("/v1/transactions/{transaction_id}", response_model=List[TransactionOut])
    async def get_transaction(
        transaction_id: str, session: AsyncSession = Depends(get_session)
    ) -> List[TransactionOut]:
        txn = await repository.get_transaction(session, transaction_id)
        if txn is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
        return [TransactionOut.model_validate(txn)]

    return app


app = create_app()
