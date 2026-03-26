import subprocess
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.api.dependencies import get_db_session
from app.core.settings import get_settings
from app.main import app
from scripts.seed_data import calculate_expected_kpis


# Use host-local database URL for host-based tests
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


def _run_script(script_name: str, args: list[str]) -> subprocess.CompletedProcess:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / script_name
    cmd = [sys.executable, str(script_path)] + args
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return result


def _run_alembic(args: list[str]) -> subprocess.CompletedProcess:
    cmd = [sys.executable, "-m", "alembic"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return result


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
        "cart_from_view_rate": round(float(row["cart_from_view_rate"]), 6),
        "order_from_cart_rate": round(float(row["order_from_cart_rate"]), 6),
        "payment_from_order_rate": round(float(row["payment_from_order_rate"]), 6),
        "payment_from_view_rate": round(float(row["payment_from_view_rate"]), 6),
    }


@pytest.fixture(scope="module")
def client():
    def _override_get_db_session():
        db_session = TestSessionLocal()
        try:
            yield db_session
        finally:
            db_session.close()

    app.dependency_overrides[get_db_session] = _override_get_db_session
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_e2e_full_flow(client):
    # 1. Migration
    res_migrate = _run_alembic(["upgrade", "head"])
    assert res_migrate.returncode == 0, res_migrate.stderr

    # 2. Seeding
    res_seed = _run_script("seed_data.py", [])
    assert res_seed.returncode == 0, res_seed.stderr
    assert "Seeding completed successfully" in res_seed.stdout

    # 3. Batch Aggregation
    res_batch = _run_script(
        "run_batch.py", ["--start-date", "2026-03-01", "--end-date", "2026-03-03"]
    )
    assert res_batch.returncode == 0, res_batch.stderr

    # 4. SQL Verification
    expected_kpis = calculate_expected_kpis()
    for d, expected in expected_kpis.items():
        # Traffic SQL
        sql_traffic = _fetch_traffic_sql_for_date(d)
        assert sql_traffic is not None
        assert sql_traffic["dau_users"] == expected["dau"]

        # Funnel SQL
        sql_funnel = _fetch_funnel_sql_for_date(d)
        assert sql_funnel is not None
        assert sql_funnel["view_users"] == expected["view_users"]
        assert sql_funnel["cart_users"] == expected["cart_users"]
        assert sql_funnel["order_users"] == expected["order_users"]
        assert sql_funnel["payment_users"] == expected["payment_users"]
        assert sql_funnel["cart_from_view_rate"] == round(
            expected["cart_from_view_rate"], 6
        )
        assert sql_funnel["order_from_cart_rate"] == round(
            expected["order_from_cart_rate"], 6
        )
        assert sql_funnel["payment_from_order_rate"] == round(
            expected["payment_from_order_rate"], 6
        )
        assert sql_funnel["payment_from_view_rate"] == round(
            expected["payment_from_view_rate"], 6
        )

    # 5. API Verification
    for d, expected in expected_kpis.items():
        # Traffic API
        resp_traffic = client.get(
            "/kpi/traffic/daily", params={"summary_date": d.isoformat()}
        )
        assert resp_traffic.status_code == 200
        assert resp_traffic.json()["dau_users"] == expected["dau"]

        # Funnel API
        resp_funnel = client.get(
            "/kpi/funnel/daily", params={"summary_date": d.isoformat()}
        )
        assert resp_funnel.status_code == 200
        data = resp_funnel.json()
        assert data["view_users"] == expected["view_users"]
        assert data["cart_users"] == expected["cart_users"]
        assert data["order_users"] == expected["order_users"]
        assert data["payment_users"] == expected["payment_users"]
        assert round(float(data["cart_from_view_rate"]), 6) == round(
            expected["cart_from_view_rate"], 6
        )
        assert round(float(data["order_from_cart_rate"]), 6) == round(
            expected["order_from_cart_rate"], 6
        )
        assert round(float(data["payment_from_order_rate"]), 6) == round(
            expected["payment_from_order_rate"], 6
        )
        assert round(float(data["payment_from_view_rate"]), 6) == round(
            expected["payment_from_view_rate"], 6
        )

    # 6. Rerun Idempotency
    res_batch_rerun = _run_script(
        "run_batch.py", ["--start-date", "2026-03-01", "--end-date", "2026-03-03"]
    )
    assert res_batch_rerun.returncode == 0, res_batch_rerun.stderr

    for d, expected in expected_kpis.items():
        sql_traffic = _fetch_traffic_sql_for_date(d)
        assert sql_traffic is not None
        assert sql_traffic["dau_users"] == expected["dau"]

        sql_funnel = _fetch_funnel_sql_for_date(d)
        assert sql_funnel is not None
        assert sql_funnel["view_users"] == expected["view_users"]
        assert sql_funnel["cart_users"] == expected["cart_users"]
        assert sql_funnel["order_users"] == expected["order_users"]
        assert sql_funnel["payment_users"] == expected["payment_users"]
