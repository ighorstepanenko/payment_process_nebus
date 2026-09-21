import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class CurrencyEnum(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentCreate(BaseModel):
    amount: Decimal = Field(..., gt=0, decimal_places=2)
    currency: CurrencyEnum
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, Any] | None = None
    webhook_url: str = Field(..., min_length=1)


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    status: str
    created_at: datetime

    model_config = {"from_attributes": True}


class PaymentDetail(BaseModel):
    id: uuid.UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any] | None
    status: str
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    model_config = {"from_attributes": True}


class PaymentEvent(BaseModel):
    """Payload published to RabbitMQ via outbox."""

    payment_id: uuid.UUID
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, Any] | None
    webhook_url: str
    idempotency_key: str
