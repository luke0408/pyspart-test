from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class ProductView(Base):
    __tablename__ = "product_views"
    __table_args__ = (
        Index("ix_product_views_user_id_viewed_at", "user_id", "viewed_at"),
        Index("ix_product_views_product_id_viewed_at", "product_id", "viewed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    product_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("products.product_id", ondelete="RESTRICT"),
        nullable=False,
    )
    viewed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    session_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class CartEvent(Base):
    __tablename__ = "cart_events"
    __table_args__ = (
        Index("ix_cart_events_user_id_added_at", "user_id", "added_at"),
        Index("ix_cart_events_product_id_added_at", "product_id", "added_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    product_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("products.product_id", ondelete="RESTRICT"),
        nullable=False,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_type: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="add"
    )
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class Order(Base):
    __tablename__ = "orders"
    __table_args__ = (
        Index("ix_orders_user_id_ordered_at", "user_id", "ordered_at"),
        Index("ix_orders_product_id_ordered_at", "product_id", "ordered_at"),
    )

    order_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    product_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("products.product_id", ondelete="RESTRICT"),
        nullable=False,
    )
    order_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(
        String(3), nullable=False, server_default="KRW"
    )
    ordered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        Index("ix_payments_user_id_paid_at", "user_id", "paid_at"),
        Index("ix_payments_status_paid_at", "payment_status", "paid_at"),
    )

    payment_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    order_id: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("orders.order_id", ondelete="RESTRICT"),
        nullable=False,
    )
    user_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payment_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    payment_status: Mapped[str] = mapped_column(String(32), nullable=False)
    payment_method: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    paid_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class DailyTrafficSummary(Base):
    __tablename__ = "daily_traffic_summary"
    __table_args__ = (
        Index("ix_daily_traffic_summary_summary_date", "summary_date"),
        Index("ix_daily_traffic_summary_aggregation_run_at", "aggregation_run_at"),
    )

    summary_date: Mapped[date] = mapped_column(Date, primary_key=True)
    dau_users: Mapped[int] = mapped_column(Integer, nullable=False)
    aggregation_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    aggregation_range_start: Mapped[date] = mapped_column(Date, nullable=False)
    aggregation_range_end: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class DailyConversionFunnel(Base):
    __tablename__ = "daily_conversion_funnel"
    __table_args__ = (
        Index("ix_daily_conversion_funnel_summary_date", "summary_date"),
        Index("ix_daily_conversion_funnel_aggregation_run_at", "aggregation_run_at"),
    )

    summary_date: Mapped[date] = mapped_column(Date, primary_key=True)
    view_users: Mapped[int] = mapped_column(Integer, nullable=False)
    cart_users: Mapped[int] = mapped_column(Integer, nullable=False)
    order_users: Mapped[int] = mapped_column(Integer, nullable=False)
    payment_users: Mapped[int] = mapped_column(Integer, nullable=False)
    cart_from_view_rate: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    order_from_cart_rate: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), nullable=False
    )
    payment_from_order_rate: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), nullable=False
    )
    payment_from_view_rate: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), nullable=False
    )
    aggregation_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    aggregation_range_start: Mapped[date] = mapped_column(Date, nullable=False)
    aggregation_range_end: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
