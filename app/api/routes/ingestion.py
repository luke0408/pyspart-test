from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_ingestion_service
from app.api.schemas.ingestion import (
    CartEventIngestionRequest,
    IngestionResponse,
    OrderIngestionRequest,
    PaymentIngestionRequest,
    ProductCreateRequest,
    ProductViewIngestionRequest,
)
from app.api.services.ingestion import IngestionService

router = APIRouter(tags=["ingestion"])


@router.post(
    "/products",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_product(
    payload: ProductCreateRequest,
    db_session: Session = Depends(get_db_session),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    product_id = ingestion_service.upsert_product(db_session, payload)
    return IngestionResponse(status="ok", resource="products", id=product_id)


@router.post(
    "/product-views",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
)
def ingest_product_view(
    payload: ProductViewIngestionRequest,
    db_session: Session = Depends(get_db_session),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    row_id = ingestion_service.create_product_view(db_session, payload)
    return IngestionResponse(status="ok", resource="product_views", id=str(row_id))


@router.post(
    "/cart-events",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
)
def ingest_cart_event(
    payload: CartEventIngestionRequest,
    db_session: Session = Depends(get_db_session),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    row_id = ingestion_service.create_cart_event(db_session, payload)
    return IngestionResponse(status="ok", resource="cart_events", id=str(row_id))


@router.post(
    "/orders",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
)
def ingest_order(
    payload: OrderIngestionRequest,
    db_session: Session = Depends(get_db_session),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    order_id = ingestion_service.upsert_order(db_session, payload)
    return IngestionResponse(status="ok", resource="orders", id=order_id)


@router.post(
    "/payments",
    response_model=IngestionResponse,
    status_code=status.HTTP_201_CREATED,
)
def ingest_payment(
    payload: PaymentIngestionRequest,
    db_session: Session = Depends(get_db_session),
    ingestion_service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    payment_id = ingestion_service.upsert_payment(db_session, payload)
    return IngestionResponse(status="ok", resource="payments", id=payment_id)
