from fastapi import FastAPI

from app.api import api_router
from app.core.exceptions import register_exception_handlers


def create_app() -> FastAPI:
    application = FastAPI(title="pyspart-test API")
    register_exception_handlers(application)
    application.include_router(api_router)
    return application


app = create_app()
