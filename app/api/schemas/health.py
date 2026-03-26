from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class DatabaseStatusResponse(BaseModel):
    status: str
    detail: Optional[str] = None


class HealthResponse(BaseModel):
    service: str
    database: DatabaseStatusResponse
    timezone: str
    checked_at: datetime
