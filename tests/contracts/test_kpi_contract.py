import subprocess
import sys
from pathlib import Path
from datetime import date

sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.seed_data import (
    DEMO_END_DATE,
    DEMO_START_DATE,
    EVENT_PEAK_DATE,
    calculate_expected_kpis,
)


def test_kpi_contract_values():
    expected = calculate_expected_kpis()

    ordered_dates = sorted(expected)
    assert ordered_dates[0] == DEMO_START_DATE
    assert ordered_dates[-1] == DEMO_END_DATE
    assert len(ordered_dates) == 31

    pre_event_dates = [day for day in ordered_dates if day < EVENT_PEAK_DATE]
    assert all(380 <= expected[day]["dau"] <= 420 for day in pre_event_dates)

    event_day = expected[EVENT_PEAK_DATE]
    assert event_day["dau"] == 800

    max_payment_from_view = max(
        expected[day]["payment_from_view_rate"] for day in ordered_dates
    )
    assert event_day["payment_from_view_rate"] == max_payment_from_view

    post_event_dates = [day for day in ordered_dates if day > EVENT_PEAK_DATE]
    assert 560 <= expected[post_event_dates[0]]["dau"] <= 610
    assert 450 <= expected[post_event_dates[-1]]["dau"] <= 500
    assert expected[post_event_dates[0]]["dau"] > expected[post_event_dates[-1]]["dau"]

    rate_keys = [
        "cart_from_view_rate",
        "order_from_cart_rate",
        "payment_from_order_rate",
        "payment_from_view_rate",
    ]
    for day in ordered_dates:
        kpi = expected[day]
        assert (
            kpi["dau"]
            >= kpi["view_users"]
            >= kpi["cart_users"]
            >= kpi["order_users"]
            >= kpi["payment_users"]
        )
        for rate_key in rate_keys:
            assert 0.0 <= kpi[rate_key] <= 1.0


def test_seed_data_print_expected_stability():
    env = {
        "DATABASE_URL": "postgresql+psycopg://postgres:postgres@localhost:5432/postgres",
        "SPARK_APP_NAME": "Test",
        "SPARK_TIMEZONE": "Asia/Seoul",
        "API_PORT": "8000",
    }
    script_path = Path(__file__).parent.parent.parent / "scripts" / "seed_data.py"

    cmd = [sys.executable, str(script_path), "--print-expected"]

    res1 = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)
    res2 = subprocess.run(cmd, capture_output=True, text=True, env=env, check=True)

    assert res1.stdout == res2.stdout
    assert res1.stdout != ""
