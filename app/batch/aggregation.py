from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, insert, text

from app.batch.date_range import enumerate_dates
from app.core.settings import get_settings
from app.core.timezone import now_in_timezone
from app.db.models import DailyConversionFunnel, DailyTrafficSummary


def _normalize_database_url(database_url: str) -> str:
    if "@postgres:5432" in database_url and not Path("/.dockerenv").exists():
        return database_url.replace("@postgres:5432", "@localhost:5432")
    return database_url


def _rate(numerator: int, denominator: int) -> Decimal:
    if denominator == 0:
        return Decimal("0.000000")
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        Decimal("0.000001"), rounding=ROUND_HALF_UP
    )


def _build_event_user_df(
    spark: Any,
    engine,
    table_name: str,
    timestamp_column: str,
    timezone_name: str,
    start_date: date,
    end_date: date,
    extra_where_sql: str = "",
) -> Any:
    where_suffix = f" AND {extra_where_sql}" if extra_where_sql else ""
    statement = text(
        f"""
        SELECT
            user_id,
            ({timestamp_column} AT TIME ZONE :timezone_name)::date AS event_date
        FROM {table_name}
        WHERE ({timestamp_column} AT TIME ZONE :timezone_name)::date BETWEEN :start_date AND :end_date
        {where_suffix}
        """
    )
    with engine.connect() as connection:
        rows = [
            {
                "user_id": row.user_id,
                "event_date": row.event_date.isoformat(),
            }
            for row in connection.execute(
                statement,
                {
                    "timezone_name": timezone_name,
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )
        ]

    if rows:
        return spark.createDataFrame(
            rows,
            schema="user_id string, event_date string",
        ).selectExpr("user_id", "to_date(event_date) as event_date")

    return spark.createDataFrame([], schema="user_id string, event_date date")


def _replace_range(
    engine,
    start_date: date,
    end_date: date,
    traffic_rows: list[dict],
    funnel_rows: list[dict],
) -> None:
    delete_statement = text(
        "DELETE FROM {table_name} WHERE summary_date BETWEEN :start_date AND :end_date"
    )
    with engine.begin() as connection:
        connection.execute(
            text(
                delete_statement.text.format(
                    table_name=DailyTrafficSummary.__tablename__,
                )
            ),
            {
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        connection.execute(
            text(
                delete_statement.text.format(
                    table_name=DailyConversionFunnel.__tablename__,
                )
            ),
            {
                "start_date": start_date,
                "end_date": end_date,
            },
        )

        if traffic_rows:
            connection.execute(insert(DailyTrafficSummary), traffic_rows)

        if funnel_rows:
            connection.execute(insert(DailyConversionFunnel), funnel_rows)


def run_daily_kpi_batch(start_date: date, end_date: date) -> None:
    from pyspark.sql import SparkSession, functions as F  # type: ignore[reportMissingImports]

    settings = get_settings()
    timezone_name = settings.SPARK_TIMEZONE
    database_url = _normalize_database_url(settings.DATABASE_URL)
    engine = create_engine(database_url, pool_pre_ping=True)

    spark = (
        SparkSession.builder.master("local[1]")
        .appName(settings.SPARK_APP_NAME)
        .config("spark.sql.session.timeZone", timezone_name)
        .getOrCreate()
    )

    try:
        view_df = _build_event_user_df(
            spark=spark,
            engine=engine,
            table_name="product_views",
            timestamp_column="viewed_at",
            timezone_name=timezone_name,
            start_date=start_date,
            end_date=end_date,
        ).dropDuplicates(["event_date", "user_id"])
        cart_df = _build_event_user_df(
            spark=spark,
            engine=engine,
            table_name="cart_events",
            timestamp_column="added_at",
            timezone_name=timezone_name,
            start_date=start_date,
            end_date=end_date,
        ).dropDuplicates(["event_date", "user_id"])
        order_df = _build_event_user_df(
            spark=spark,
            engine=engine,
            table_name="orders",
            timestamp_column="ordered_at",
            timezone_name=timezone_name,
            start_date=start_date,
            end_date=end_date,
        ).dropDuplicates(["event_date", "user_id"])
        payment_all_df = _build_event_user_df(
            spark=spark,
            engine=engine,
            table_name="payments",
            timestamp_column="paid_at",
            timezone_name=timezone_name,
            start_date=start_date,
            end_date=end_date,
        ).dropDuplicates(["event_date", "user_id"])
        payment_completed_df = _build_event_user_df(
            spark=spark,
            engine=engine,
            table_name="payments",
            timestamp_column="paid_at",
            timezone_name=timezone_name,
            start_date=start_date,
            end_date=end_date,
            extra_where_sql="payment_status = 'completed'",
        ).dropDuplicates(["event_date", "user_id"])

        cart_with_view_df = cart_df.join(
            view_df,
            on=["event_date", "user_id"],
            how="inner",
        ).dropDuplicates(["event_date", "user_id"])
        order_with_prior_df = order_df.join(
            cart_with_view_df, on=["event_date", "user_id"], how="inner"
        ).dropDuplicates(["event_date", "user_id"])
        payment_with_prior_df = payment_completed_df.join(
            order_with_prior_df,
            on=["event_date", "user_id"],
            how="inner",
        ).dropDuplicates(["event_date", "user_id"])

        all_users_df = (
            view_df.select("event_date", "user_id")
            .unionByName(cart_df.select("event_date", "user_id"))
            .unionByName(order_df.select("event_date", "user_id"))
            .unionByName(payment_all_df.select("event_date", "user_id"))
            .dropDuplicates(["event_date", "user_id"])
        )

        dau_by_day = {
            row.event_date: int(row.user_count)
            for row in all_users_df.groupBy("event_date")
            .agg(F.countDistinct("user_id").alias("user_count"))
            .collect()
        }
        view_by_day = {
            row.event_date: int(row.user_count)
            for row in view_df.groupBy("event_date")
            .agg(F.countDistinct("user_id").alias("user_count"))
            .collect()
        }
        cart_by_day = {
            row.event_date: int(row.user_count)
            for row in cart_with_view_df.groupBy("event_date")
            .agg(F.countDistinct("user_id").alias("user_count"))
            .collect()
        }
        order_by_day = {
            row.event_date: int(row.user_count)
            for row in order_with_prior_df.groupBy("event_date")
            .agg(F.countDistinct("user_id").alias("user_count"))
            .collect()
        }
        payment_by_day = {
            row.event_date: int(row.user_count)
            for row in payment_with_prior_df.groupBy("event_date")
            .agg(F.countDistinct("user_id").alias("user_count"))
            .collect()
        }

        aggregation_run_at = now_in_timezone(timezone_name)
        traffic_rows: list[dict] = []
        funnel_rows: list[dict] = []

        target_dates = enumerate_dates(start_date=start_date, end_date=end_date)

        for summary_date in target_dates:
            view_users = view_by_day.get(summary_date, 0)
            cart_users = cart_by_day.get(summary_date, 0)
            order_users = order_by_day.get(summary_date, 0)
            payment_users = payment_by_day.get(summary_date, 0)

            traffic_rows = [
                *traffic_rows,
                {
                    "summary_date": summary_date,
                    "dau_users": dau_by_day.get(summary_date, 0),
                    "aggregation_run_at": aggregation_run_at,
                    "aggregation_range_start": start_date,
                    "aggregation_range_end": end_date,
                },
            ]
            funnel_rows = [
                *funnel_rows,
                {
                    "summary_date": summary_date,
                    "view_users": view_users,
                    "cart_users": cart_users,
                    "order_users": order_users,
                    "payment_users": payment_users,
                    "cart_from_view_rate": _rate(cart_users, view_users),
                    "order_from_cart_rate": _rate(order_users, cart_users),
                    "payment_from_order_rate": _rate(payment_users, order_users),
                    "payment_from_view_rate": _rate(payment_users, view_users),
                    "aggregation_run_at": aggregation_run_at,
                    "aggregation_range_start": start_date,
                    "aggregation_range_end": end_date,
                },
            ]

        _replace_range(
            engine=engine,
            start_date=start_date,
            end_date=end_date,
            traffic_rows=traffic_rows,
            funnel_rows=funnel_rows,
        )
    finally:
        spark.stop()
        engine.dispose()
