from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from app.api.routes.health import router as health_router
from app.api.routes.report import router as report_router
from app.core.settings import get_settings


def _mark_export_upload_required(openapi_schema: dict[str, Any], export_path: str) -> None:
    operation = openapi_schema.get("paths", {}).get(export_path, {}).get("post")
    if not isinstance(operation, dict):
        return

    request_body = operation.get("requestBody")
    if not isinstance(request_body, dict):
        return

    request_body["required"] = True

    multipart_schema = (
        request_body.get("content", {})
        .get("multipart/form-data", {})
        .get("schema", {})
    )
    if not isinstance(multipart_schema, dict):
        return

    schema_ref = multipart_schema.get("$ref")
    if not isinstance(schema_ref, str):
        multipart_schema.update(
            {
                "type": "object",
                "required": ["file"],
                "properties": {
                    "file": {
                        "title": "File",
                        "type": "string",
                        "format": "binary",
                    }
                },
            }
        )
        return

    component_name = schema_ref.rsplit("/", maxsplit=1)[-1]
    body_schema = openapi_schema.get("components", {}).get("schemas", {}).get(component_name)
    if not isinstance(body_schema, dict):
        return

    body_schema["type"] = "object"
    body_schema["required"] = ["file"]
    body_schema["properties"] = {
        "file": {
            "title": "File",
            "type": "string",
            "format": "binary",
        }
    }


def create_app() -> FastAPI:
    settings = get_settings()
    # Step 1 runtime precondition: both API and worker must use the same absolute
    # shared root; here we only enforce local path validity and prepare the root.
    Path(settings.shared_jobs_root).mkdir(parents=True, exist_ok=True)
    app = FastAPI(title=settings.app_name, debug=settings.debug)
    app.include_router(health_router)
    app.include_router(report_router, prefix=settings.api_prefix)

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema is not None:
            return app.openapi_schema

        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        _mark_export_upload_required(openapi_schema, f"{settings.api_prefix}/report/export")
        app.openapi_schema = openapi_schema
        return openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
    return app


app = create_app()
