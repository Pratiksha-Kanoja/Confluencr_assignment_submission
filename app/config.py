from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./transactions.db"

    processing_delay_seconds: float = 30.0
    lease_seconds: float = 120.0
    max_attempts: int = 5
    retry_backoff_seconds: float = 5.0
    worker_concurrency: int = 200
    worker_poll_interval_seconds: float = 2.0
    worker_enabled: bool = True

    log_level: str = "INFO"

    @model_validator(mode="after")
    def _normalize(self) -> "Settings":
        raw = (self.database_url or "").strip()
        # Render/UI copy-pastes sometimes include surrounding quotes; strip them.
        raw = raw.strip('"').strip("'")
        # Whitespace inside a URL almost always means it was pasted incorrectly
        # (e.g. includes `psql ...` or line breaks). Fail fast with a clear error.
        if any(ch.isspace() for ch in raw):
            raise ValueError(
                "DATABASE_URL contains whitespace/newlines. Paste only the raw URI (no quotes, no psql prefix)."
            )

        # Managed Postgres providers hand out postgres:// or postgresql:// URLs;
        # SQLAlchemy needs the async driver spelled out.
        url = raw
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://"):]
        if url.startswith("postgresql://"):
            url = "postgresql+asyncpg://" + url[len("postgresql://"):]

        # Validate without leaking secrets to logs/exceptions.
        try:
            parsed = make_url(url)
        except Exception as e:  # pragma: no cover (depends on external URL parser details)
            raise ValueError("DATABASE_URL is not a valid SQLAlchemy URL") from e
        if parsed.get_backend_name() == "postgresql":
            host = parsed.host or ""
            if not host:
                raise ValueError("DATABASE_URL is missing a hostname")
            if any(ch.isspace() for ch in host) or "/" in host:
                raise ValueError(
                    f"DATABASE_URL hostname is invalid: {host!r}. Check for copy/paste artifacts."
                )

        self.database_url = url

        if self.lease_seconds <= self.processing_delay_seconds:
            raise ValueError("LEASE_SECONDS must be greater than PROCESSING_DELAY_SECONDS")
        return self

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")


@lru_cache
def get_settings() -> Settings:
    return Settings()
