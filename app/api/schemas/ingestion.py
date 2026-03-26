from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PaymentStatus(str, Enum):
    completed = "completed"
    pending = "pending"
    failed = "failed"


def _require_timezone_aware_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("timestamp must include timezone offset")
    return value


class ProductCreateRequest(BaseModel):
    product_id: str = Field(min_length=1, max_length=64)
    product_name: str = Field(min_length=1, max_length=255)
    category: str = Field(min_length=1, max_length=128)
    price: Decimal = Field(gt=0)


class ProductViewIngestionRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    product_id: str = Field(min_length=1, max_length=64)
    viewed_at: datetime
    session_id: Optional[str] = Field(default=None, max_length=128)

    @field_validator("viewed_at")
    @classmethod
    def validate_viewed_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_timestamp(value)


class CartEventIngestionRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=64)
    product_id: str = Field(min_length=1, max_length=64)
    quantity: int = Field(gt=0)
    added_at: datetime
    event_type: Optional[str] = Field(default="add", min_length=1, max_length=32)

    @field_validator("added_at")
    @classmethod
    def validate_added_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_timestamp(value)


class OrderIngestionRequest(BaseModel):
    order_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=64)
    product_id: str = Field(min_length=1, max_length=64)
    order_amount: Decimal = Field(gt=0)
    ordered_at: datetime
    currency: str = Field(default="KRW", min_length=3, max_length=3)

    @field_validator("ordered_at")
    @classmethod
    def validate_ordered_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_timestamp(value)


class PaymentIngestionRequest(BaseModel):
    payment_id: str = Field(min_length=1, max_length=64)
    order_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=64)
    payment_amount: Decimal = Field(gt=0)
    payment_status: PaymentStatus
    paid_at: datetime
    payment_method: Optional[str] = Field(default=None, max_length=32)

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("paid_at")
    @classmethod
    def validate_paid_at(cls, value: datetime) -> datetime:
        return _require_timezone_aware_timestamp(value)


class IngestionResponse(BaseModel):
    status: str
    resource: str
    id: str
