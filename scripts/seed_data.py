import json
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Set
import argparse

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
import zoneinfo

sys.path.append(str(Path(__file__).parent.parent))

from app.core.settings import get_settings
from app.db.models import Product, ProductView, CartEvent, Order, Payment

settings = get_settings()


def get_db_url() -> str:
    url = settings.DATABASE_URL
    if "@postgres:5432" in url and not Path("/.dockerenv").exists():
        return url.replace("@postgres:5432", "@localhost:5432")
    return url


engine = create_engine(get_db_url())
SessionLocal = sessionmaker(bind=engine)

DATA_DIR = Path(__file__).parent.parent / "data"
TZ = zoneinfo.ZoneInfo(settings.SPARK_TIMEZONE)


def load_json(filename: str) -> List[Dict[str, Any]]:
    with open(DATA_DIR / filename, "r") as f:
        return json.load(f)


def get_local_date(dt_str: str) -> date:
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ).date()


def calculate_expected_kpis() -> Dict[date, Dict[str, Any]]:
    views = load_json("product_views.json")
    carts = load_json("cart_events.json")
    orders = load_json("orders.json")
    payments = load_json("payments.json")

    dates: Set[date] = set()
    for v in views:
        dates.add(get_local_date(v["viewed_at"]))
    for c in carts:
        dates.add(get_local_date(c["added_at"]))
    for o in orders:
        dates.add(get_local_date(o["ordered_at"]))
    for p in payments:
        dates.add(get_local_date(p["paid_at"]))

    expected = {}
    for d in sorted(list(dates)):
        day_views = {v["user_id"] for v in views if get_local_date(v["viewed_at"]) == d}
        day_carts = {c["user_id"] for c in carts if get_local_date(c["added_at"]) == d}
        day_orders = {
            o["user_id"] for o in orders if get_local_date(o["ordered_at"]) == d
        }
        day_payments_all = {
            p["user_id"] for p in payments if get_local_date(p["paid_at"]) == d
        }
        day_payments_completed = {
            p["user_id"]
            for p in payments
            if get_local_date(p["paid_at"]) == d and p["payment_status"] == "completed"
        }

        dau = day_views | day_carts | day_orders | day_payments_all

        view_users = day_views
        cart_users = day_carts & view_users
        order_users = day_orders & cart_users
        payment_users = day_payments_completed & order_users

        def rate(num, den):
            return float(len(num)) / len(den) if len(den) > 0 else 0.0

        expected[d] = {
            "dau": len(dau),
            "view_users": len(view_users),
            "cart_users": len(cart_users),
            "order_users": len(order_users),
            "payment_users": len(payment_users),
            "cart_from_view_rate": rate(cart_users, view_users),
            "order_from_cart_rate": rate(order_users, cart_users),
            "payment_from_order_rate": rate(payment_users, order_users),
            "payment_from_view_rate": rate(payment_users, view_users),
        }
    return expected


def seed_data():
    db: Session = SessionLocal()
    try:
        db.execute(
            text(
                "TRUNCATE TABLE payments, orders, cart_events, product_views, products CASCADE"
            )
        )
        db.commit()

        products = load_json("products.json")
        for p in products:
            db.add(Product(**p))
        db.commit()

        views = load_json("product_views.json")
        for v in views:
            v_copy = v.copy()
            v_copy["viewed_at"] = datetime.fromisoformat(v_copy["viewed_at"])
            db.add(ProductView(**v_copy))
        db.commit()

        carts = load_json("cart_events.json")
        for c in carts:
            c_copy = c.copy()
            c_copy["added_at"] = datetime.fromisoformat(c_copy["added_at"])
            db.add(CartEvent(**c_copy))
        db.commit()

        orders = load_json("orders.json")
        for o in orders:
            o_copy = o.copy()
            o_copy["ordered_at"] = datetime.fromisoformat(o_copy["ordered_at"])
            db.add(Order(**o_copy))
        db.commit()

        payments = load_json("payments.json")
        for p in payments:
            p_copy = p.copy()
            p_copy["paid_at"] = datetime.fromisoformat(p_copy["paid_at"])
            db.add(Payment(**p_copy))
        db.commit()

        print("Seeding completed successfully.")
    except Exception as e:
        db.rollback()
        print(f"Error during seeding: {e}")
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-expected", action="store_true")
    args = parser.parse_args()

    if args.print_expected:
        expected = calculate_expected_kpis()
        for d, kpis in expected.items():
            print(f"Date: {d}")
            print(f"  DAU: {kpis['dau']}")
            print(
                f"  Funnel: {kpis['view_users']} -> {kpis['cart_users']} -> {kpis['order_users']} -> {kpis['payment_users']}"
            )
            print(
                f"  Rates: CV={kpis['cart_from_view_rate']:.4f}, OC={kpis['order_from_cart_rate']:.4f}, PO={kpis['payment_from_order_rate']:.4f}, PV={kpis['payment_from_view_rate']:.4f}"
            )
    else:
        seed_data()
