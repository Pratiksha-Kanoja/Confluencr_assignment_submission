from typing import Any, Dict, Tuple

import logging
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import Settings

logger = logging.getLogger("app.db")


class Base(DeclarativeBase):
    pass


def _prepare_url(raw_url: str) -> Tuple[URL, Dict[str, Any]]:
    """asyncpg rejects libpq-only query params (sslmode, channel_binding) that
    Neon/Supabase include in their connection strings, so translate them."""
    url = make_url(raw_url)
    connect_args: Dict[str, Any] = {}
    if url.get_backend_name() == "postgresql":
        query = dict(url.query)
        sslmode = query.pop("sslmode", None)
        query.pop("channel_binding", None)
        # Supabase Supavisor pooler + asyncpg: prepared statement caching can
        # break under pooling. Supabase recommends disabling it.
        statement_cache_size = query.pop("statement_cache_size", None)
        if statement_cache_size is not None:
            try:
                connect_args["statement_cache_size"] = int(statement_cache_size)
            except ValueError:
                logger.warning("Ignoring invalid statement_cache_size=%r in DATABASE_URL", statement_cache_size)
        elif (url.host or "").endswith(".pooler.supabase.com"):
            connect_args["statement_cache_size"] = 0
        if sslmode and sslmode != "disable":
            # asyncpg expects ssl as bool/SSLContext; libpq-style sslmode strings
            # are not accepted directly. For managed providers (Neon/Supabase),
            # any non-disable sslmode effectively means "use TLS".
            connect_args["ssl"] = True
        url = url.set(query=query)
    return url, connect_args


def create_engine(settings: Settings) -> AsyncEngine:
    url, connect_args = _prepare_url(settings.database_url)
    logger.info("DB engine url=%s", url.render_as_string(hide_password=True))
    kwargs: Dict[str, Any] = {"connect_args": connect_args}
    if url.get_backend_name() == "postgresql":
        kwargs.update(pool_size=10, max_overflow=20, pool_pre_ping=True, pool_recycle=300)
    else:
        connect_args.setdefault("timeout", 30)
    return create_async_engine(url, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def init_db(engine: AsyncEngine) -> None:
    from app import models  # noqa: F401  (register tables on Base.metadata)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
