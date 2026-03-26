import subprocess
import sys
from pathlib import Path
from datetime import date

sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.seed_data import calculate_expected_kpis


def test_kpi_contract_values():
    expected = calculate_expected_kpis()

    assert date(2026, 3, 1) in expected
    d1 = expected[date(2026, 3, 1)]
    assert d1["dau"] == 5
    assert d1["view_users"] == 5
    assert d1["cart_users"] == 3
    assert d1["order_users"] == 2
    assert d1["payment_users"] == 1
    assert round(d1["cart_from_view_rate"], 4) == 0.6000
    assert round(d1["order_from_cart_rate"], 4) == 0.6667
    assert round(d1["payment_from_order_rate"], 4) == 0.5000
    assert round(d1["payment_from_view_rate"], 4) == 0.2000

    assert date(2026, 3, 2) in expected
    d2 = expected[date(2026, 3, 2)]
    assert d2["dau"] == 1
    assert d2["view_users"] == 1
    assert d2["cart_users"] == 1
    assert d2["order_users"] == 1
    assert d2["payment_users"] == 1

    assert date(2026, 3, 3) in expected
    d3 = expected[date(2026, 3, 3)]
    assert d3["dau"] == 2
    assert d3["view_users"] == 1
    assert d3["cart_users"] == 0
    assert d3["order_users"] == 0
    assert d3["payment_users"] == 0


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
