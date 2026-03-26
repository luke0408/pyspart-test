from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import DailyConversionFunnel, DailyTrafficSummary


class KpiReadService:
    def get_daily_traffic(
        self, db_session: Session, summary_date: date
    ) -> Optional[DailyTrafficSummary]:
        statement = select(DailyTrafficSummary).where(
            DailyTrafficSummary.summary_date == summary_date
        )
        return db_session.execute(statement).scalar_one_or_none()

    def get_ranged_traffic(
        self, db_session: Session, start_date: date, end_date: date
    ) -> list[DailyTrafficSummary]:
        statement = (
            select(DailyTrafficSummary)
            .where(DailyTrafficSummary.summary_date >= start_date)
            .where(DailyTrafficSummary.summary_date <= end_date)
            .order_by(DailyTrafficSummary.summary_date.asc())
        )
        return list(db_session.execute(statement).scalars())

    def get_daily_funnel(
        self, db_session: Session, summary_date: date
    ) -> Optional[DailyConversionFunnel]:
        statement = select(DailyConversionFunnel).where(
            DailyConversionFunnel.summary_date == summary_date
        )
        return db_session.execute(statement).scalar_one_or_none()

    def get_ranged_funnel(
        self, db_session: Session, start_date: date, end_date: date
    ) -> list[DailyConversionFunnel]:
        statement = (
            select(DailyConversionFunnel)
            .where(DailyConversionFunnel.summary_date >= start_date)
            .where(DailyConversionFunnel.summary_date <= end_date)
            .order_by(DailyConversionFunnel.summary_date.asc())
        )
        return list(db_session.execute(statement).scalars())
