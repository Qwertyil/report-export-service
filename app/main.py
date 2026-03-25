from pathlib import Path

from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.report import router as report_router
from app.core.settings import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    # Step 1 runtime precondition: both API and worker must use the same absolute
    # shared root; here we only enforce local path validity and prepare the root.
    Path(settings.shared_jobs_root).mkdir(parents=True, exist_ok=True)
    app = FastAPI(title=settings.app_name, debug=settings.debug)
    app.include_router(health_router)
    app.include_router(report_router, prefix=settings.api_prefix)
    return app


app = create_app()
