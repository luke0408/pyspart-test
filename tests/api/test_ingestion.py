import json
from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.api.dependencies import get_db_session
from app.core.settings import get_settings
from app.main import app

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


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


def _load_json(path: str) -> list[dict]:
    return json.loads((DATA_DIR / path).read_text())


def _truncate_raw_tables() -> None:
    with TestSessionLocal() as db_session:
        db_session.execute(
            text(
                "TRUNCATE TABLE payments, orders, cart_events, product_views, products CASCADE"
            )
        )
        db_session.commit()


def _count_rows(db_session: Session, table_name: str) -> int:
    return int(
        db_session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one()
    )


@pytest.fixture(autouse=True)
def clean_raw_tables() -> Generator[None, None, None]:
    _truncate_raw_tables()
    yield
    _truncate_raw_tables()


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    def _override_get_db_session() -> Generator[Session, None, None]:
        db_session = TestSessionLocal()
        try:
            yield db_session
        finally:
            db_session.close()

    app.dependency_overrides[get_db_session] = _override_get_db_session
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_ingestion_inserts_fixture_payloads_and_counts(client: TestClient) -> None:
    products = _load_json("products.json")
    product_views = _load_json("product_views.json")
    cart_events = _load_json("cart_events.json")
    orders = _load_json("orders.json")
    payments = _load_json("payments.json")

    for product in products:
        response = client.post("/products", json=product)
        assert response.status_code == 201
        assert response.json()["resource"] == "products"

    for product_view in product_views:
        response = client.post("/product-views", json=product_view)
        assert response.status_code == 201
        assert response.json()["resource"] == "product_views"

    for cart_event in cart_events:
        response = client.post("/cart-events", json=cart_event)
        assert response.status_code == 201
        assert response.json()["resource"] == "cart_events"

    for order in orders:
        response = client.post("/orders", json=order)
        assert response.status_code == 201
        assert response.json()["resource"] == "orders"

    for payment in payments:
        response = client.post("/payments", json=payment)
        assert response.status_code == 201
        assert response.json()["resource"] == "payments"

    with TestSessionLocal() as db_session:
        assert _count_rows(db_session, "products") == len(products)
        assert _count_rows(db_session, "product_views") == len(product_views)
        assert _count_rows(db_session, "cart_events") == len(cart_events)
        assert _count_rows(db_session, "orders") == len(orders)
        assert _count_rows(db_session, "payments") == len(payments)


def test_upsert_on_products_orders_and_payments_keeps_natural_key_counts(
    client: TestClient,
) -> None:
    product_payload = {
        "product_id": "prod_same",
        "product_name": "Original",
        "category": "Books",
        "price": 1000.0,
    }
    response = client.post("/products", json=product_payload)
    assert response.status_code == 201

    updated_product_payload = {
        **product_payload,
        "product_name": "Updated",
        "price": 1200.0,
    }
    response = client.post("/products", json=updated_product_payload)
    assert response.status_code == 201

    order_payload = {
        "order_id": "ord_same",
        "user_id": "user_same",
        "product_id": "prod_same",
        "order_amount": 1200.0,
        "currency": "KRW",
        "ordered_at": "2026-03-01T10:10:00+09:00",
    }
    response = client.post("/orders", json=order_payload)
    assert response.status_code == 201

    updated_order_payload = {
        **order_payload,
        "order_amount": 1300.0,
    }
    response = client.post("/orders", json=updated_order_payload)
    assert response.status_code == 201

    payment_payload = {
        "payment_id": "pay_same",
        "order_id": "ord_same",
        "user_id": "user_same",
        "payment_amount": 1300.0,
        "payment_status": "completed",
        "payment_method": "card",
        "paid_at": "2026-03-01T10:15:00+09:00",
    }
    response = client.post("/payments", json=payment_payload)
    assert response.status_code == 201

    updated_payment_payload = {
        **payment_payload,
        "payment_status": "pending",
    }
    response = client.post("/payments", json=updated_payment_payload)
    assert response.status_code == 201

    with TestSessionLocal() as db_session:
        assert _count_rows(db_session, "products") == 1
        assert _count_rows(db_session, "orders") == 1
        assert _count_rows(db_session, "payments") == 1


def test_invalid_payment_payload_returns_deterministic_validation_errors(
    client: TestClient,
) -> None:
    payload = {
        "payment_id": "pay_bad",
        "order_id": "ord_bad",
        "user_id": "user_bad",
        "payment_amount": -1,
        "payment_status": "unknown",
        "payment_method": "card",
        "paid_at": "2026-03-01T10:15:00+09:00",
    }

    response = client.post("/payments", json=payload)

    assert response.status_code == 422
    body = response.json()
    assert body["detail"] == "validation_error"
    error_fields = {error["field"] for error in body["errors"]}
    assert "body.payment_amount" in error_fields
    assert "body.payment_status" in error_fields


def test_missing_fields_and_bad_timestamp_return_deterministic_validation_errors(
    client: TestClient,
) -> None:
    missing_field_payload = {
        "product_name": "No ID Product",
        "category": "Books",
        "price": 1000.0,
    }
    missing_field_response = client.post("/products", json=missing_field_payload)
    assert missing_field_response.status_code == 422
    missing_field_body = missing_field_response.json()
    assert missing_field_body["detail"] == "validation_error"
    assert any(
        error["field"] == "body.product_id" for error in missing_field_body["errors"]
    )

    bad_timestamp_payload = {
        "user_id": "user_1",
        "product_id": "prod_1",
        "viewed_at": "2026-03-01T10:00:00",
    }
    bad_timestamp_response = client.post("/product-views", json=bad_timestamp_payload)
    assert bad_timestamp_response.status_code == 422
    bad_timestamp_body = bad_timestamp_response.json()
    assert bad_timestamp_body["detail"] == "validation_error"
    assert any(
        error["field"] == "body.viewed_at" and "timezone offset" in error["message"]
        for error in bad_timestamp_body["errors"]
    )


def test_foreign_key_violation_returns_deterministic_integrity_error(
    client: TestClient,
) -> None:
    payload = {
        "user_id": "user_1",
        "product_id": "missing_product",
        "quantity": 1,
        "added_at": "2026-03-01T10:05:00+09:00",
        "event_type": "add",
    }

    response = client.post("/cart-events", json=payload)

    assert response.status_code == 409
    assert response.json() == {
        "detail": "integrity_error",
        "message": "foreign_key_violation",
    }
