from typing import Union

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.api.dependencies import get_health_service
from app.api.schemas.health import HealthResponse
from app.api.services.health import HealthService

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def read_health(
    health_service: HealthService = Depends(get_health_service),
) -> Union[JSONResponse, HealthResponse]:
    payload, status_code = health_service.check()
    if status_code == 200:
        return payload

    return JSONResponse(
        status_code=status_code, content=payload.model_dump(mode="json")
    )
