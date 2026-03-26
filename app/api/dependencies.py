from app.api.services.health import HealthService
from app.api.services.ingestion import IngestionService
from app.api.services.kpi import KpiReadService
from app.core.settings import get_settings
from app.core.timezone import now_in_timezone
from app.db.session import get_db_session, probe_database


def get_health_service() -> HealthService:
    settings = get_settings()
    return HealthService(
        database_probe=probe_database,
        timezone_name=settings.SPARK_TIMEZONE,
        clock=now_in_timezone,
    )


def get_ingestion_service() -> IngestionService:
    return IngestionService()


def get_kpi_read_service() -> KpiReadService:
    return KpiReadService()


__all__ = [
    "get_db_session",
    "get_health_service",
    "get_ingestion_service",
    "get_kpi_read_service",
]
