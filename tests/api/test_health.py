from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from app.api.dependencies import get_health_service
from app.api.services.health import HealthService
from app.main import app


def _fixed_clock(timezone_name: str) -> datetime:
    return datetime(2026, 3, 26, 10, 0, 0, tzinfo=ZoneInfo(timezone_name))


def _build_health_service(database_ok: bool) -> HealthService:
    def _probe() -> tuple[bool, Optional[str]]:
        if database_ok:
            return True, None
        return False, "database_unavailable"

    return HealthService(
        database_probe=_probe,
        timezone_name="Asia/Seoul",
        clock=_fixed_clock,
    )


def test_health_returns_200_when_database_is_healthy() -> None:
    app.dependency_overrides[get_health_service] = lambda: _build_health_service(True)
    with TestClient(app) as client:
        response = client.get("/health")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "service": "ok",
        "database": {"status": "healthy", "detail": None},
        "timezone": "Asia/Seoul",
        "checked_at": "2026-03-26T10:00:00+09:00",
    }


def test_health_returns_503_when_database_is_unavailable() -> None:
    app.dependency_overrides[get_health_service] = lambda: _build_health_service(False)
    with TestClient(app) as client:
        response = client.get("/health")
    app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json() == {
        "service": "degraded",
        "database": {
            "status": "unhealthy",
            "detail": "database_unavailable",
        },
        "timezone": "Asia/Seoul",
        "checked_at": "2026-03-26T10:00:00+09:00",
    }
