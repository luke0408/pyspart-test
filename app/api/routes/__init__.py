from app.api.routes.health import router as health_router
from app.api.routes.ingestion import router as ingestion_router
from app.api.routes.kpi import router as kpi_router

__all__ = ["health_router", "ingestion_router", "kpi_router"]
