from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import SQLAlchemyError


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = [
            {
                "field": ".".join(str(item) for item in error.get("loc", [])),
                "message": error.get("msg", "invalid_input"),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"detail": "validation_error", "errors": errors},
        )

    @app.exception_handler(IntegrityError)
    async def integrity_exception_handler(
        _request: Request, exc: IntegrityError
    ) -> JSONResponse:
        pg_error_code = getattr(exc.orig, "sqlstate", None) or getattr(
            exc.orig, "pgcode", None
        )
        if pg_error_code == "23503":
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "integrity_error",
                    "message": "foreign_key_violation",
                },
            )

        return JSONResponse(
            status_code=409,
            content={"detail": "integrity_error", "message": "constraint_violation"},
        )

    @app.exception_handler(SQLAlchemyError)
    async def sqlalchemy_exception_handler(
        _request: Request, _exc: SQLAlchemyError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": "database_unavailable"},
        )
