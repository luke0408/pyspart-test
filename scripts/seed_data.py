from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
import zoneinfo

sys.path.append(str(Path(__file__).parent.parent))

from app.batch.date_range import enumerate_dates
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

DEMO_START_DATE = date(2026, 3, 1)
DEMO_END_DATE = date(2026, 3, 31)
DEMO_MONTHLY_ACTIVE_USERS = 1000

EVENT_PEAK_DATE = date(2026, 3, 16)
PRE_EVENT_DAILY_USERS = 400
PEAK_EVENT_DAILY_USERS = 800
POST_EVENT_RETENTION_START_DAU = 600
POST_EVENT_RETENTION_END_DAU = 460


def load_json(filename: str) -> list[dict[str, Any]]:
    with open(DATA_DIR / filename, "r") as f:
        return json.load(f)


def get_local_date(dt_str: str) -> date:
    dt = datetime.fromisoformat(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ).date()


def _daily_target_users(target_date: date) -> int:
    if target_date < EVENT_PEAK_DATE:
        oscillation = ((target_date.day * 17) % 31) - 15
        return PRE_EVENT_DAILY_USERS + oscillation

    if target_date == EVENT_PEAK_DATE:
        return PEAK_EVENT_DAILY_USERS

    days_after_event = (target_date - EVENT_PEAK_DATE).days
    total_post_event_days = (DEMO_END_DATE - EVENT_PEAK_DATE).days
    if total_post_event_days <= 1:
        baseline = POST_EVENT_RETENTION_END_DAU
    else:
        progress = (days_after_event - 1) / (total_post_event_days - 1)
        baseline = round(
            POST_EVENT_RETENTION_START_DAU
            - (POST_EVENT_RETENTION_START_DAU - POST_EVENT_RETENTION_END_DAU) * progress
        )

    oscillation = ((target_date.day * 11) % 17) - 8
    return max(POST_EVENT_RETENTION_END_DAU, baseline + oscillation)


def _daily_funnel_ratios(target_date: date) -> tuple[float, float, float]:
    if target_date < EVENT_PEAK_DATE:
        cart_from_view = 0.34 + ((target_date.day % 4) * 0.01)
        order_from_cart = 0.50 + ((target_date.day % 3) * 0.01)
        payment_from_order = 0.70 + ((target_date.day % 2) * 0.01)
        return cart_from_view, order_from_cart, payment_from_order

    if target_date == EVENT_PEAK_DATE:
        return 0.52, 0.66, 0.87

    days_after_event = (target_date - EVENT_PEAK_DATE).days
    total_post_event_days = (DEMO_END_DATE - EVENT_PEAK_DATE).days
    progress = 1.0
    if total_post_event_days > 1:
        progress = (days_after_event - 1) / (total_post_event_days - 1)

    cart_from_view = 0.44 - (0.08 * progress)
    order_from_cart = 0.60 - (0.09 * progress)
    payment_from_order = 0.80 - (0.10 * progress)
    return cart_from_view, order_from_cart, payment_from_order


def _iso_timestamp_in_tz(target_date: date, minute_offset: int) -> str:
    base = datetime.combine(target_date, time(hour=9, minute=0), tzinfo=TZ)
    return (base + timedelta(minutes=minute_offset)).isoformat()


def _build_demo_events() -> dict[str, list[dict[str, Any]]]:
    products = load_json("products.json")
    if not products:
        raise ValueError("products.json must contain at least one product")

    product_ids = [str(product["product_id"]) for product in products]
    price_by_product = {
        str(product["product_id"]): float(product["price"]) for product in products
    }

    demo_dates = enumerate_dates(start_date=DEMO_START_DATE, end_date=DEMO_END_DATE)
    monthly_user_pool = [
        f"user_{user_index:04d}"
        for user_index in range(1, DEMO_MONTHLY_ACTIVE_USERS + 1)
    ]

    mandatory_users_by_day: dict[date, list[str]] = {
        target_date: [] for target_date in demo_dates
    }
    for user_index, user_id in enumerate(monthly_user_pool):
        mandatory_date = demo_dates[user_index % len(demo_dates)]
        mandatory_users_by_day[mandatory_date].append(user_id)

    product_views: list[dict[str, Any]] = []
    cart_events: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []

    order_sequence = 1
    payment_sequence = 1

    for day_index, target_date in enumerate(demo_dates):
        target_users = min(
            DEMO_MONTHLY_ACTIVE_USERS,
            max(1, _daily_target_users(target_date)),
        )

        daily_users = [*mandatory_users_by_day[target_date]]
        daily_user_set = set(daily_users)
        rotation_start = (day_index * 37) % len(monthly_user_pool)
        rotation_cursor = 0
        while len(daily_users) < target_users:
            candidate = monthly_user_pool[
                (rotation_start + rotation_cursor) % len(monthly_user_pool)
            ]
            if candidate not in daily_user_set:
                daily_users.append(candidate)
                daily_user_set.add(candidate)
            rotation_cursor += 1

        user_product_map = {
            user_id: product_ids[(day_index + user_position) % len(product_ids)]
            for user_position, user_id in enumerate(daily_users)
        }

        cart_ratio, order_ratio, payment_ratio = _daily_funnel_ratios(target_date)
        cart_count = min(len(daily_users), max(1, round(len(daily_users) * cart_ratio)))
        cart_users = daily_users[:cart_count]
        order_count = min(cart_count, max(1, round(cart_count * order_ratio)))
        order_users = cart_users[:order_count]

        payment_completed_count = min(
            order_count,
            max(1, round(order_count * payment_ratio)),
        )
        if order_count > 1 and payment_completed_count == order_count:
            payment_completed_count -= 1

        payment_pending_count = min(
            order_count - payment_completed_count,
            max(0, round(order_count * 0.12)),
        )
        if order_count > payment_completed_count and payment_pending_count == 0:
            payment_pending_count = 1

        for user_position, user_id in enumerate(daily_users):
            product_views.append(
                {
                    "user_id": user_id,
                    "product_id": user_product_map[user_id],
                    "viewed_at": _iso_timestamp_in_tz(target_date, user_position),
                    "session_id": f"sess_{target_date.strftime('%Y%m%d')}_{user_position:04d}",
                }
            )

        for user_position, user_id in enumerate(cart_users):
            cart_events.append(
                {
                    "user_id": user_id,
                    "product_id": user_product_map[user_id],
                    "quantity": 1 + ((day_index + user_position) % 2),
                    "added_at": _iso_timestamp_in_tz(target_date, user_position + 3),
                    "event_type": "add",
                }
            )

        day_orders: list[dict[str, Any]] = []
        for user_position, user_id in enumerate(order_users):
            product_id = user_product_map[user_id]
            order_id = f"ord_{order_sequence:06d}"
            order_sequence += 1
            order_row = {
                "order_id": order_id,
                "user_id": user_id,
                "product_id": product_id,
                "order_amount": price_by_product[product_id],
                "currency": "KRW",
                "ordered_at": _iso_timestamp_in_tz(target_date, user_position + 6),
            }
            orders.append(order_row)
            day_orders.append(order_row)

        for order_position, order_row in enumerate(day_orders):
            payment_status: str | None = None
            if order_position < payment_completed_count:
                payment_status = "completed"
            elif order_position < (payment_completed_count + payment_pending_count):
                payment_status = "pending"

            if payment_status is None:
                continue

            payments.append(
                {
                    "payment_id": f"pay_{payment_sequence:06d}",
                    "order_id": order_row["order_id"],
                    "user_id": order_row["user_id"],
                    "payment_amount": order_row["order_amount"],
                    "payment_status": payment_status,
                    "payment_method": "card",
                    "paid_at": _iso_timestamp_in_tz(target_date, order_position + 9),
                }
            )
            payment_sequence += 1

    return {
        "product_views": product_views,
        "cart_events": cart_events,
        "orders": orders,
        "payments": payments,
    }


def calculate_expected_kpis(
    event_payloads: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[date, dict[str, Any]]:
    payloads = event_payloads if event_payloads is not None else _build_demo_events()
    views = payloads["product_views"]
    carts = payloads["cart_events"]
    orders = payloads["orders"]
    payments = payloads["payments"]

    dates = set(enumerate_dates(DEMO_START_DATE, DEMO_END_DATE))
    for view_event in views:
        dates.add(get_local_date(view_event["viewed_at"]))
    for cart_event in carts:
        dates.add(get_local_date(cart_event["added_at"]))
    for order_event in orders:
        dates.add(get_local_date(order_event["ordered_at"]))
    for payment_event in payments:
        dates.add(get_local_date(payment_event["paid_at"]))

    expected: dict[date, dict[str, Any]] = {}
    for summary_date in sorted(dates):
        day_views = {
            v["user_id"]
            for v in views
            if get_local_date(v["viewed_at"]) == summary_date
        }
        day_carts = {
            c["user_id"] for c in carts if get_local_date(c["added_at"]) == summary_date
        }
        day_orders = {
            o["user_id"]
            for o in orders
            if get_local_date(o["ordered_at"]) == summary_date
        }
        day_payments_all = {
            p["user_id"]
            for p in payments
            if get_local_date(p["paid_at"]) == summary_date
        }
        day_payments_completed = {
            p["user_id"]
            for p in payments
            if get_local_date(p["paid_at"]) == summary_date
            and p["payment_status"] == "completed"
        }

        dau = day_views | day_carts | day_orders | day_payments_all

        view_users = day_views
        cart_users = day_carts & view_users
        order_users = day_orders & cart_users
        payment_users = day_payments_completed & order_users

        def rate(num, den):
            return float(len(num)) / len(den) if len(den) > 0 else 0.0

        expected[summary_date] = {
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


def calculate_dashboard_summaries() -> dict[str, Any]:
    payloads = _build_demo_events()
    expected = calculate_expected_kpis(event_payloads=payloads)
    ordered_dates = sorted(expected)

    traffic_summary = [
        {
            "summary_date": summary_date.isoformat(),
            "dau_users": expected[summary_date]["dau"],
        }
        for summary_date in ordered_dates
    ]

    funnel_summary = [
        {
            "summary_date": summary_date.isoformat(),
            "view_users": expected[summary_date]["view_users"],
            "cart_users": expected[summary_date]["cart_users"],
            "order_users": expected[summary_date]["order_users"],
            "payment_users": expected[summary_date]["payment_users"],
        }
        for summary_date in ordered_dates
    ]

    aggregation_rate = [
        {
            "summary_date": summary_date.isoformat(),
            "cart_from_view_rate": round(
                expected[summary_date]["cart_from_view_rate"],
                6,
            ),
            "order_from_cart_rate": round(
                expected[summary_date]["order_from_cart_rate"],
                6,
            ),
            "payment_from_order_rate": round(
                expected[summary_date]["payment_from_order_rate"],
                6,
            ),
            "payment_from_view_rate": round(
                expected[summary_date]["payment_from_view_rate"],
                6,
            ),
        }
        for summary_date in ordered_dates
    ]

    monthly_active_users = {
        event["user_id"]
        for table_name in ("product_views", "cart_events", "orders", "payments")
        for event in payloads[table_name]
    }
    expected_days = len(enumerate_dates(DEMO_START_DATE, DEMO_END_DATE))
    covered_days = len(ordered_dates)

    aggregation_coverage = {
        "aggregation_range_start": DEMO_START_DATE.isoformat(),
        "aggregation_range_end": DEMO_END_DATE.isoformat(),
        "covered_days": covered_days,
        "expected_days": expected_days,
        "coverage_rate": round(covered_days / expected_days, 4),
        "monthly_active_users": len(monthly_active_users),
        "peak_summary_date": EVENT_PEAK_DATE.isoformat(),
        "peak_dau_users": expected[EVENT_PEAK_DATE]["dau"],
    }

    return {
        "traffic_summary": traffic_summary,
        "funnel_summary": funnel_summary,
        "aggregation_rate": aggregation_rate,
        "aggregation_coverage": aggregation_coverage,
    }


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

        event_payloads = _build_demo_events()

        views = event_payloads["product_views"]
        for v in views:
            v_copy = v.copy()
            v_copy["viewed_at"] = datetime.fromisoformat(v_copy["viewed_at"])
            db.add(ProductView(**v_copy))
        db.commit()

        carts = event_payloads["cart_events"]
        for c in carts:
            c_copy = c.copy()
            c_copy["added_at"] = datetime.fromisoformat(c_copy["added_at"])
            db.add(CartEvent(**c_copy))
        db.commit()

        orders = event_payloads["orders"]
        for o in orders:
            o_copy = o.copy()
            o_copy["ordered_at"] = datetime.fromisoformat(o_copy["ordered_at"])
            db.add(Order(**o_copy))
        db.commit()

        payments = event_payloads["payments"]
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
    parser.add_argument("--print-dashboard-summaries", action="store_true")
    args = parser.parse_args()

    if args.print_dashboard_summaries:
        print(
            json.dumps(
                calculate_dashboard_summaries(),
                ensure_ascii=False,
                indent=2,
            )
        )
    elif args.print_expected:
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
