from celery import Celery

from app.core.settings import get_settings

settings = get_settings()
REPORT_EXPORT_TASK_NAME = "report.process_job"

celery_app = Celery(
    "report_export_service",
    broker=settings.effective_celery_broker_url,
    backend=settings.effective_celery_result_backend,
)

celery_app.conf.task_default_queue = "report-export"


def enqueue_report_job(job_id: str) -> None:
    # The broker publish is fire-and-forget here; API-visible state lives only in
    # the job repository and not in the AsyncResult payload/result backend.
    celery_app.send_task(
        REPORT_EXPORT_TASK_NAME,
        kwargs={"job_id": job_id},
        queue=celery_app.conf.task_default_queue,
    )


# Import for task registration side effects (must follow celery_app construction).
from app.workers import report_process_job as _report_process_job  # noqa: E402, F401
