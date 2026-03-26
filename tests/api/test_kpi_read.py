import subprocess
import sys
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.api.dependencies import get_db_session
from app.core.settings import get_settings
from app.main import app
from scripts.seed_data import seed_data


def _host_database_url() -> str:
    database_url = get_settings().DATABASE_URL
    if "@postgres:5432" in database_url:
        return database_url.replace("@postgres:5432", "@localhost:5432")
    return database_url


engine = create_engine(_host_database_url(), pool_pre_ping=True)
TestSessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


def _truncate_all_tables() -> None:
    with TestSessionLocal() as db_session:
        db_session.execute(
            text(
                "TRUNCATE TABLE daily_conversion_funnel, daily_traffic_summary, payments, orders, cart_events, product_views, products CASCADE"
            )
        )
        db_session.commit()


def _run_batch_script(start_date: str, end_date: str) -> None:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "run_batch.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _decimal_to_six(value: Decimal) -> float:
    return round(float(value), 6)


def _fetch_traffic_sql_for_date(summary_date: date) -> Optional[dict]:
    with TestSessionLocal() as db_session:
        row = (
            db_session.execute(
                text(
                    """
                SELECT summary_date, dau_users
                FROM daily_traffic_summary
                WHERE summary_date = :summary_date
                """
                ),
                {"summary_date": summary_date},
            )
            .mappings()
            .first()
        )
    if row is None:
        return None
    return {
        "summary_date": row["summary_date"].isoformat(),
        "dau_users": row["dau_users"],
    }


def _fetch_traffic_sql_for_range(start_date: date, end_date: date) -> list[dict]:
    with TestSessionLocal() as db_session:
        rows = db_session.execute(
            text(
                """
                SELECT summary_date, dau_users
                FROM daily_traffic_summary
                WHERE summary_date BETWEEN :start_date AND :end_date
                ORDER BY summary_date ASC
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings()
        return [
            {
                "summary_date": row["summary_date"].isoformat(),
                "dau_users": row["dau_users"],
            }
            for row in rows
        ]


def _fetch_funnel_sql_for_date(summary_date: date) -> Optional[dict]:
    with TestSessionLocal() as db_session:
        row = (
            db_session.execute(
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
                    payment_from_view_rate
                FROM daily_conversion_funnel
                WHERE summary_date = :summary_date
                """
                ),
                {"summary_date": summary_date},
            )
            .mappings()
            .first()
        )
    if row is None:
        return None
    return {
        "summary_date": row["summary_date"].isoformat(),
        "view_users": row["view_users"],
        "cart_users": row["cart_users"],
        "order_users": row["order_users"],
        "payment_users": row["payment_users"],
        "cart_from_view_rate": _decimal_to_six(row["cart_from_view_rate"]),
        "order_from_cart_rate": _decimal_to_six(row["order_from_cart_rate"]),
        "payment_from_order_rate": _decimal_to_six(row["payment_from_order_rate"]),
        "payment_from_view_rate": _decimal_to_six(row["payment_from_view_rate"]),
    }


def _fetch_funnel_sql_for_range(start_date: date, end_date: date) -> list[dict]:
    with TestSessionLocal() as db_session:
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
                    payment_from_view_rate
                FROM daily_conversion_funnel
                WHERE summary_date BETWEEN :start_date AND :end_date
                ORDER BY summary_date ASC
                """
            ),
            {"start_date": start_date, "end_date": end_date},
        ).mappings()
        return [
            {
                "summary_date": row["summary_date"].isoformat(),
                "view_users": row["view_users"],
                "cart_users": row["cart_users"],
                "order_users": row["order_users"],
                "payment_users": row["payment_users"],
                "cart_from_view_rate": _decimal_to_six(row["cart_from_view_rate"]),
                "order_from_cart_rate": _decimal_to_six(row["order_from_cart_rate"]),
                "payment_from_order_rate": _decimal_to_six(
                    row["payment_from_order_rate"]
                ),
                "payment_from_view_rate": _decimal_to_six(
                    row["payment_from_view_rate"]
                ),
            }
            for row in rows
        ]


def _normalize_funnel_payload(payload: dict) -> dict:
    return {
        "summary_date": payload["summary_date"],
        "view_users": payload["view_users"],
        "cart_users": payload["cart_users"],
        "order_users": payload["order_users"],
        "payment_users": payload["payment_users"],
        "cart_from_view_rate": round(float(payload["cart_from_view_rate"]), 6),
        "order_from_cart_rate": round(float(payload["order_from_cart_rate"]), 6),
        "payment_from_order_rate": round(float(payload["payment_from_order_rate"]), 6),
        "payment_from_view_rate": round(float(payload["payment_from_view_rate"]), 6),
    }


def _normalize_funnel_list_payload(payload: list[dict]) -> list[dict]:
    return [_normalize_funnel_payload(item) for item in payload]


def _is_date_ascending(items: list[dict]) -> bool:
    dates = [item["summary_date"] for item in items]
    return dates == sorted(dates)


def _seed_and_aggregate() -> None:
    seed_data()
    _run_batch_script("2026-03-01", "2026-03-03")


def _override_get_db_session() -> Generator[Session, None, None]:
    db_session = TestSessionLocal()
    try:
        yield db_session
    finally:
        db_session.close()


def _make_client() -> TestClient:
    app.dependency_overrides[get_db_session] = _override_get_db_session
    return TestClient(app)


def _clear_overrides() -> None:
    app.dependency_overrides.clear()


def setup_function() -> None:
    _truncate_all_tables()


def teardown_function() -> None:
    _clear_overrides()
    _truncate_all_tables()


def test_daily_traffic_endpoint_matches_summary_sql() -> None:
    _seed_and_aggregate()
    expected = _fetch_traffic_sql_for_date(date(2026, 3, 1))
    assert expected is not None

    with _make_client() as client:
        response = client.get(
            "/kpi/traffic/daily", params={"summary_date": "2026-03-01"}
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_ranged_traffic_endpoint_matches_summary_sql_and_is_ascending() -> None:
    _seed_and_aggregate()
    expected = _fetch_traffic_sql_for_range(date(2026, 3, 1), date(2026, 3, 3))

    with _make_client() as client:
        response = client.get(
            "/kpi/traffic/range",
            params={"start_date": "2026-03-01", "end_date": "2026-03-03"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert _is_date_ascending(response.json())


def test_daily_funnel_endpoint_matches_summary_sql() -> None:
    _seed_and_aggregate()
    expected = _fetch_funnel_sql_for_date(date(2026, 3, 1))
    assert expected is not None

    with _make_client() as client:
        response = client.get(
            "/kpi/funnel/daily", params={"summary_date": "2026-03-01"}
        )

    assert response.status_code == 200
    assert _normalize_funnel_payload(response.json()) == expected


def test_ranged_funnel_endpoint_matches_summary_sql_and_is_ascending() -> None:
    _seed_and_aggregate()
    expected = _fetch_funnel_sql_for_range(date(2026, 3, 1), date(2026, 3, 3))

    with _make_client() as client:
        response = client.get(
            "/kpi/funnel/range",
            params={"start_date": "2026-03-01", "end_date": "2026-03-03"},
        )

    assert response.status_code == 200
    assert _normalize_funnel_list_payload(response.json()) == expected
    assert _is_date_ascending(response.json())


def test_kpi_range_and_date_validation_errors_are_deterministic() -> None:
    _seed_and_aggregate()

    with _make_client() as client:
        invalid_range_response = client.get(
            "/kpi/traffic/range",
            params={"start_date": "2026-03-03", "end_date": "2026-03-01"},
        )
        invalid_date_response = client.get(
            "/kpi/funnel/daily",
            params={"summary_date": "2026-13-01"},
        )

    assert invalid_range_response.status_code == 422
    assert invalid_range_response.json() == {"detail": "start_date must be <= end_date"}

    assert invalid_date_response.status_code == 422
    assert invalid_date_response.json() == {
        "detail": "summary_date must be in YYYY-MM-DD format"
    }
