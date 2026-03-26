from datetime import date
from decimal import Decimal

from pydantic import BaseModel


class DailyTrafficKpiResponse(BaseModel):
    summary_date: date
    dau_users: int


class DailyConversionFunnelKpiResponse(BaseModel):
    summary_date: date
    view_users: int
    cart_users: int
    order_users: int
    payment_users: int
    cart_from_view_rate: Decimal
    order_from_cart_rate: Decimal
    payment_from_order_rate: Decimal
    payment_from_view_rate: Decimal
