from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.dependencies import get_db_session, get_kpi_read_service
from app.api.schemas.kpi import (
    DailyConversionFunnelKpiResponse,
    DailyTrafficKpiResponse,
)
from app.api.services.kpi import KpiReadService

router = APIRouter(prefix="/kpi", tags=["kpi"])


def _parse_iso_date_or_422(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must be in YYYY-MM-DD format",
        ) from exc


def _parse_range_or_422(start_date: str, end_date: str) -> tuple[date, date]:
    parsed_start_date = _parse_iso_date_or_422(start_date, "start_date")
    parsed_end_date = _parse_iso_date_or_422(end_date, "end_date")
    if parsed_start_date > parsed_end_date:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")
    return parsed_start_date, parsed_end_date


@router.get("/traffic/daily", response_model=DailyTrafficKpiResponse)
def read_daily_traffic_kpi(
    summary_date: str = Query(...),
    db_session: Session = Depends(get_db_session),
    kpi_read_service: KpiReadService = Depends(get_kpi_read_service),
) -> DailyTrafficKpiResponse:
    parsed_summary_date = _parse_iso_date_or_422(summary_date, "summary_date")
    row = kpi_read_service.get_daily_traffic(db_session, parsed_summary_date)
    if row is None:
        raise HTTPException(status_code=404, detail="traffic summary not found")
    return DailyTrafficKpiResponse(
        summary_date=row.summary_date,
        dau_users=row.dau_users,
    )


@router.get("/traffic/range", response_model=list[DailyTrafficKpiResponse])
def read_ranged_traffic_kpi(
    start_date: str = Query(...),
    end_date: str = Query(...),
    db_session: Session = Depends(get_db_session),
    kpi_read_service: KpiReadService = Depends(get_kpi_read_service),
) -> list[DailyTrafficKpiResponse]:
    parsed_start_date, parsed_end_date = _parse_range_or_422(start_date, end_date)
    rows = kpi_read_service.get_ranged_traffic(
        db_session,
        parsed_start_date,
        parsed_end_date,
    )
    return [
        DailyTrafficKpiResponse(summary_date=row.summary_date, dau_users=row.dau_users)
        for row in rows
    ]


@router.get("/funnel/daily", response_model=DailyConversionFunnelKpiResponse)
def read_daily_funnel_kpi(
    summary_date: str = Query(...),
    db_session: Session = Depends(get_db_session),
    kpi_read_service: KpiReadService = Depends(get_kpi_read_service),
) -> DailyConversionFunnelKpiResponse:
    parsed_summary_date = _parse_iso_date_or_422(summary_date, "summary_date")
    row = kpi_read_service.get_daily_funnel(db_session, parsed_summary_date)
    if row is None:
        raise HTTPException(status_code=404, detail="funnel summary not found")
    return DailyConversionFunnelKpiResponse(
        summary_date=row.summary_date,
        view_users=row.view_users,
        cart_users=row.cart_users,
        order_users=row.order_users,
        payment_users=row.payment_users,
        cart_from_view_rate=row.cart_from_view_rate,
        order_from_cart_rate=row.order_from_cart_rate,
        payment_from_order_rate=row.payment_from_order_rate,
        payment_from_view_rate=row.payment_from_view_rate,
    )


@router.get("/funnel/range", response_model=list[DailyConversionFunnelKpiResponse])
def read_ranged_funnel_kpi(
    start_date: str = Query(...),
    end_date: str = Query(...),
    db_session: Session = Depends(get_db_session),
    kpi_read_service: KpiReadService = Depends(get_kpi_read_service),
) -> list[DailyConversionFunnelKpiResponse]:
    parsed_start_date, parsed_end_date = _parse_range_or_422(start_date, end_date)
    rows = kpi_read_service.get_ranged_funnel(
        db_session,
        parsed_start_date,
        parsed_end_date,
    )
    return [
        DailyConversionFunnelKpiResponse(
            summary_date=row.summary_date,
            view_users=row.view_users,
            cart_users=row.cart_users,
            order_users=row.order_users,
            payment_users=row.payment_users,
            cart_from_view_rate=row.cart_from_view_rate,
            order_from_cart_rate=row.order_from_cart_rate,
            payment_from_order_rate=row.payment_from_order_rate,
            payment_from_view_rate=row.payment_from_view_rate,
        )
        for row in rows
    ]
