from collections.abc import Callable
from datetime import datetime
from typing import Optional

from app.api.schemas.health import DatabaseStatusResponse, HealthResponse


class HealthService:
    def __init__(
        self,
        database_probe: Callable[[], tuple[bool, Optional[str]]],
        timezone_name: str,
        clock: Callable[[str], datetime],
    ) -> None:
        self._database_probe = database_probe
        self._timezone_name = timezone_name
        self._clock = clock

    def check(self) -> tuple[HealthResponse, int]:
        database_ok, database_detail = self._database_probe()
        checked_at = self._clock(self._timezone_name)

        if database_ok:
            return (
                HealthResponse(
                    service="ok",
                    database=DatabaseStatusResponse(status="healthy"),
                    timezone=self._timezone_name,
                    checked_at=checked_at,
                ),
                200,
            )

        return (
            HealthResponse(
                service="degraded",
                database=DatabaseStatusResponse(
                    status="unhealthy",
                    detail=database_detail or "database_unavailable",
                ),
                timezone=self._timezone_name,
                checked_at=checked_at,
            ),
            503,
        )
