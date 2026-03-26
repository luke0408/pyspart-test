from __future__ import annotations

import subprocess
import sys
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.batch.date_range import parse_inclusive_date_range
from app.core.settings import get_settings
from scripts.seed_data import calculate_expected_kpis, seed_data


def _host_database_url() -> str:
    database_url = get_settings().DATABASE_URL
    if "@postgres:5432" in database_url:
        return database_url.replace("@postgres:5432", "@localhost:5432")
    return database_url


engine = create_engine(_host_database_url(), pool_pre_ping=True)
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


def _truncate_all_tables() -> None:
    with SessionLocal() as db_session:
        db_session.execute(
            text(
                "TRUNCATE TABLE daily_conversion_funnel, daily_traffic_summary, payments, orders, cart_events, product_views, products CASCADE"
            )
        )
        db_session.commit()


def _fetch_traffic_summary(db_session: Session) -> list[dict]:
    rows = db_session.execute(
        text(
            """
            SELECT summary_date, dau_users, aggregation_range_start, aggregation_range_end
            FROM daily_traffic_summary
            ORDER BY summary_date ASC
            """
        )
    ).mappings()
    return [dict(row) for row in rows]


def _fetch_funnel_summary(db_session: Session) -> list[dict]:
    rows = db_session.execute(
        text(
            """
            SELECT
                summary_date,
                view_users,
                cart_users,
                order_users,
                payment_users,
                cart_from_view_rate,
                order_from_cart_rate,
                payment_from_order_rate,
                payment_from_view_rate,
                aggregation_range_start,
                aggregation_range_end
            FROM daily_conversion_funnel
            ORDER BY summary_date ASC
            """
        )
    ).mappings()
    return [dict(row) for row in rows]


def _as_decimal_rate(value: float) -> Decimal:
    return Decimal(f"{value:.6f}")


def _run_batch_script(
    start_date: str, end_date: str
) -> subprocess.CompletedProcess[str]:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "run_batch.py"
    command = [
        sys.executable,
        str(script_path),
        "--start-date",
        start_date,
        "--end-date",
        end_date,
    ]
    return subprocess.run(command, capture_output=True, text=True, check=False)


@pytest.fixture(autouse=True)
def clean_all_tables() -> Generator[None, None, None]:
    _truncate_all_tables()
    yield
    _truncate_all_tables()


def test_batch_happy_path_matches_seed_contract() -> None:
    seed_data()
    result = _run_batch_script("2026-03-01", "2026-03-03")
    assert result.returncode == 0, result.stderr

    expected = calculate_expected_kpis()
    with SessionLocal() as db_session:
        traffic_rows = _fetch_traffic_summary(db_session)
        funnel_rows = _fetch_funnel_summary(db_session)

    assert len(traffic_rows) == 3
    assert len(funnel_rows) == 3

    for traffic_row in traffic_rows:
        summary_date = traffic_row["summary_date"]
        contract = expected[summary_date]
        assert traffic_row["dau_users"] == contract["dau"]
        assert traffic_row["aggregation_range_start"] == date(2026, 3, 1)
        assert traffic_row["aggregation_range_end"] == date(2026, 3, 3)

    for funnel_row in funnel_rows:
        summary_date = funnel_row["summary_date"]
        contract = expected[summary_date]
        assert funnel_row["view_users"] == contract["view_users"]
        assert funnel_row["cart_users"] == contract["cart_users"]
        assert funnel_row["order_users"] == contract["order_users"]
        assert funnel_row["payment_users"] == contract["payment_users"]
        assert funnel_row["cart_from_view_rate"] == _as_decimal_rate(
            contract["cart_from_view_rate"]
        )
        assert funnel_row["order_from_cart_rate"] == _as_decimal_rate(
            contract["order_from_cart_rate"]
        )
        assert funnel_row["payment_from_order_rate"] == _as_decimal_rate(
            contract["payment_from_order_rate"]
        )
        assert funnel_row["payment_from_view_rate"] == _as_decimal_rate(
            contract["payment_from_view_rate"]
        )
        assert funnel_row["aggregation_range_start"] == date(2026, 3, 1)
        assert funnel_row["aggregation_range_end"] == date(2026, 3, 3)


def test_batch_rerun_replaces_range_without_duplication() -> None:
    seed_data()
    range_start = date(2026, 3, 1)
    range_end = date(2026, 3, 3)

    first_run = _run_batch_script(str(range_start), str(range_end))
    assert first_run.returncode == 0, first_run.stderr
    with SessionLocal() as db_session:
        first_traffic = _fetch_traffic_summary(db_session)
        first_funnel = _fetch_funnel_summary(db_session)

    second_run = _run_batch_script(str(range_start), str(range_end))
    assert second_run.returncode == 0, second_run.stderr
    with SessionLocal() as db_session:
        second_traffic = _fetch_traffic_summary(db_session)
        second_funnel = _fetch_funnel_summary(db_session)

    assert len(first_traffic) == len(second_traffic) == 3
    assert len(first_funnel) == len(second_funnel) == 3

    for first_row, second_row in zip(first_traffic, second_traffic):
        assert first_row["summary_date"] == second_row["summary_date"]
        assert first_row["dau_users"] == second_row["dau_users"]

    funnel_fields = [
        "summary_date",
        "view_users",
        "cart_users",
        "order_users",
        "payment_users",
        "cart_from_view_rate",
        "order_from_cart_rate",
        "payment_from_order_rate",
        "payment_from_view_rate",
    ]
    for first_row, second_row in zip(first_funnel, second_funnel):
        for field in funnel_fields:
            assert first_row[field] == second_row[field]


def test_batch_invalid_date_range_fails_deterministically() -> None:
    with pytest.raises(ValueError, match="start_date must be <= end_date"):
        parse_inclusive_date_range("2026-03-03", "2026-03-01")

    result = _run_batch_script("2026-03-03", "2026-03-01")

    assert result.returncode != 0
    assert "start_date must be <= end_date" in result.stderr
