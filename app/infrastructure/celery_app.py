from celery import Celery

from app.core.settings import get_settings

settings = get_settings()

celery_app = Celery(
    "report_export_service",
    broker=settings.effective_celery_broker_url,
    backend=settings.effective_celery_result_backend,
)

celery_app.conf.task_default_queue = "report-export"
