from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


def to_utc_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class TransactionWebhook(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    transaction_id: str = Field(min_length=1, max_length=128)
    source_account: str = Field(min_length=1, max_length=128)
    destination_account: str = Field(min_length=1, max_length=128)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(min_length=3, max_length=3)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, value: str) -> str:
        if not value.isalpha():
            raise ValueError("currency must be a 3-letter ISO 4217 code")
        return value.upper()


class WebhookAck(BaseModel):
    status: str = "ACCEPTED"
    transaction_id: str
    duplicate: bool


class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    status: str
    created_at: datetime
    processed_at: Optional[datetime] = None

    @field_serializer("amount")
    def _amount(self, value: Decimal) -> float:
        return float(value)

    @field_serializer("created_at", "processed_at")
    def _timestamps(self, value: Optional[datetime]) -> Optional[str]:
        return to_utc_iso(value)


class HealthOut(BaseModel):
    status: str
    current_time: str
