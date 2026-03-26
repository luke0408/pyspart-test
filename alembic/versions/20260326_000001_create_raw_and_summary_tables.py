from collections.abc import Sequence
from typing import Optional, Union

from alembic import op
import sqlalchemy as sa

revision: str = "20260326_000001"
down_revision: Optional[str] = None
branch_labels: Optional[Union[str, Sequence[str]]] = None
depends_on: Optional[Union[str, Sequence[str]]] = None


def upgrade() -> None:
    op.create_table(
        "products",
        sa.Column("product_id", sa.String(length=64), nullable=False),
        sa.Column("product_name", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=True),
        sa.Column("price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("product_id"),
    )

    op.create_table(
        "daily_traffic_summary",
        sa.Column("summary_date", sa.Date(), nullable=False),
        sa.Column("dau_users", sa.Integer(), nullable=False),
        sa.Column("aggregation_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("aggregation_range_start", sa.Date(), nullable=False),
        sa.Column("aggregation_range_end", sa.Date(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("summary_date"),
    )
    op.create_index(
        "ix_daily_traffic_summary_summary_date",
        "daily_traffic_summary",
        ["summary_date"],
        unique=False,
    )
    op.create_index(
        "ix_daily_traffic_summary_aggregation_run_at",
        "daily_traffic_summary",
        ["aggregation_run_at"],
        unique=False,
    )

    op.create_table(
        "daily_conversion_funnel",
        sa.Column("summary_date", sa.Date(), nullable=False),
        sa.Column("view_users", sa.Integer(), nullable=False),
        sa.Column("cart_users", sa.Integer(), nullable=False),
        sa.Column("order_users", sa.Integer(), nullable=False),
        sa.Column("payment_users", sa.Integer(), nullable=False),
        sa.Column(
            "cart_from_view_rate", sa.Numeric(precision=10, scale=6), nullable=False
        ),
        sa.Column(
            "order_from_cart_rate", sa.Numeric(precision=10, scale=6), nullable=False
        ),
        sa.Column(
            "payment_from_order_rate", sa.Numeric(precision=10, scale=6), nullable=False
        ),
        sa.Column(
            "payment_from_view_rate", sa.Numeric(precision=10, scale=6), nullable=False
        ),
        sa.Column("aggregation_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("aggregation_range_start", sa.Date(), nullable=False),
        sa.Column("aggregation_range_end", sa.Date(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("summary_date"),
    )
    op.create_index(
        "ix_daily_conversion_funnel_summary_date",
        "daily_conversion_funnel",
        ["summary_date"],
        unique=False,
    )
    op.create_index(
        "ix_daily_conversion_funnel_aggregation_run_at",
        "daily_conversion_funnel",
        ["aggregation_run_at"],
        unique=False,
    )

    op.create_table(
        "product_views",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("product_id", sa.String(length=64), nullable=False),
        sa.Column("viewed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["product_id"], ["products.product_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_product_views_user_id_viewed_at",
        "product_views",
        ["user_id", "viewed_at"],
        unique=False,
    )
    op.create_index(
        "ix_product_views_product_id_viewed_at",
        "product_views",
        ["product_id", "viewed_at"],
        unique=False,
    )

    op.create_table(
        "cart_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("product_id", sa.String(length=64), nullable=False),
        sa.Column("quantity", sa.Integer(), server_default="1", nullable=False),
        sa.Column("added_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "event_type", sa.String(length=32), server_default="add", nullable=False
        ),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["product_id"], ["products.product_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_cart_events_user_id_added_at",
        "cart_events",
        ["user_id", "added_at"],
        unique=False,
    )
    op.create_index(
        "ix_cart_events_product_id_added_at",
        "cart_events",
        ["product_id", "added_at"],
        unique=False,
    )

    op.create_table(
        "orders",
        sa.Column("order_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("product_id", sa.String(length=64), nullable=False),
        sa.Column("order_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column(
            "currency", sa.String(length=3), server_default="KRW", nullable=False
        ),
        sa.Column("ordered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["product_id"], ["products.product_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("order_id"),
    )
    op.create_index(
        "ix_orders_user_id_ordered_at",
        "orders",
        ["user_id", "ordered_at"],
        unique=False,
    )
    op.create_index(
        "ix_orders_product_id_ordered_at",
        "orders",
        ["product_id", "ordered_at"],
        unique=False,
    )

    op.create_table(
        "payments",
        sa.Column("payment_id", sa.String(length=64), nullable=False),
        sa.Column("order_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("payment_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("payment_status", sa.String(length=32), nullable=False),
        sa.Column("payment_method", sa.String(length=32), nullable=True),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["order_id"], ["orders.order_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("payment_id"),
    )
    op.create_index(
        "ix_payments_user_id_paid_at",
        "payments",
        ["user_id", "paid_at"],
        unique=False,
    )
    op.create_index(
        "ix_payments_status_paid_at",
        "payments",
        ["payment_status", "paid_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_payments_status_paid_at", table_name="payments")
    op.drop_index("ix_payments_user_id_paid_at", table_name="payments")
    op.drop_table("payments")

    op.drop_index("ix_orders_product_id_ordered_at", table_name="orders")
    op.drop_index("ix_orders_user_id_ordered_at", table_name="orders")
    op.drop_table("orders")

    op.drop_index("ix_cart_events_product_id_added_at", table_name="cart_events")
    op.drop_index("ix_cart_events_user_id_added_at", table_name="cart_events")
    op.drop_table("cart_events")

    op.drop_index("ix_product_views_product_id_viewed_at", table_name="product_views")
    op.drop_index("ix_product_views_user_id_viewed_at", table_name="product_views")
    op.drop_table("product_views")

    op.drop_index(
        "ix_daily_conversion_funnel_aggregation_run_at",
        table_name="daily_conversion_funnel",
    )
    op.drop_index(
        "ix_daily_conversion_funnel_summary_date",
        table_name="daily_conversion_funnel",
    )
    op.drop_table("daily_conversion_funnel")

    op.drop_index(
        "ix_daily_traffic_summary_aggregation_run_at",
        table_name="daily_traffic_summary",
    )
    op.drop_index(
        "ix_daily_traffic_summary_summary_date",
        table_name="daily_traffic_summary",
    )
    op.drop_table("daily_traffic_summary")

    op.drop_table("products")
