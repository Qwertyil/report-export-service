from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.report import router as report_router
from app.core.settings import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, debug=settings.debug)
    app.include_router(health_router)
    app.include_router(report_router, prefix=settings.api_prefix)
    return app


app = create_app()
