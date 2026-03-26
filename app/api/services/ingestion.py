from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.schemas.ingestion import (
    CartEventIngestionRequest,
    OrderIngestionRequest,
    PaymentIngestionRequest,
    ProductCreateRequest,
    ProductViewIngestionRequest,
)
from app.db.models import CartEvent, Order, Payment, Product, ProductView


class IngestionService:
    def upsert_product(self, db_session: Session, payload: ProductCreateRequest) -> str:
        statement = insert(Product).values(
            product_id=payload.product_id,
            product_name=payload.product_name,
            category=payload.category,
            price=payload.price,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[Product.product_id],
            set_={
                "product_name": payload.product_name,
                "category": payload.category,
                "price": payload.price,
            },
        )
        try:
            db_session.execute(statement)
            db_session.commit()
            return payload.product_id
        except IntegrityError:
            db_session.rollback()
            raise

    def create_product_view(
        self,
        db_session: Session,
        payload: ProductViewIngestionRequest,
    ) -> int:
        product_view = ProductView(
            user_id=payload.user_id,
            product_id=payload.product_id,
            viewed_at=payload.viewed_at,
            session_id=payload.session_id,
        )
        try:
            db_session.add(product_view)
            db_session.commit()
            db_session.refresh(product_view)
            return product_view.id
        except IntegrityError:
            db_session.rollback()
            raise

    def create_cart_event(
        self,
        db_session: Session,
        payload: CartEventIngestionRequest,
    ) -> int:
        cart_event = CartEvent(
            user_id=payload.user_id,
            product_id=payload.product_id,
            quantity=payload.quantity,
            added_at=payload.added_at,
            event_type=payload.event_type or "add",
        )
        try:
            db_session.add(cart_event)
            db_session.commit()
            db_session.refresh(cart_event)
            return cart_event.id
        except IntegrityError:
            db_session.rollback()
            raise

    def upsert_order(self, db_session: Session, payload: OrderIngestionRequest) -> str:
        statement = insert(Order).values(
            order_id=payload.order_id,
            user_id=payload.user_id,
            product_id=payload.product_id,
            order_amount=payload.order_amount,
            currency=payload.currency,
            ordered_at=payload.ordered_at,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[Order.order_id],
            set_={
                "user_id": payload.user_id,
                "product_id": payload.product_id,
                "order_amount": payload.order_amount,
                "currency": payload.currency,
                "ordered_at": payload.ordered_at,
            },
        )
        try:
            db_session.execute(statement)
            db_session.commit()
            return payload.order_id
        except IntegrityError:
            db_session.rollback()
            raise

    def upsert_payment(
        self, db_session: Session, payload: PaymentIngestionRequest
    ) -> str:
        statement = insert(Payment).values(
            payment_id=payload.payment_id,
            order_id=payload.order_id,
            user_id=payload.user_id,
            payment_amount=payload.payment_amount,
            payment_status=payload.payment_status,
            payment_method=payload.payment_method,
            paid_at=payload.paid_at,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[Payment.payment_id],
            set_={
                "order_id": payload.order_id,
                "user_id": payload.user_id,
                "payment_amount": payload.payment_amount,
                "payment_status": payload.payment_status,
                "payment_method": payload.payment_method,
                "paid_at": payload.paid_at,
            },
        )
        try:
            db_session.execute(statement)
            db_session.commit()
            return payload.payment_id
        except IntegrityError:
            db_session.rollback()
            raise
