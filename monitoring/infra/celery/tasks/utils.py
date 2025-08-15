# core/tasks/utils.py
from celery import shared_task

from monitoring.infra.celery.tasks.base import LockingTask


def locking_task(*, max_retries: int, default_retry_delay: int):
    """
    @locking_task(max_retries=5, default_retry_delay=10)
    def foo(self, task_id): ...
    """
    return shared_task(
        bind=True,
        base=LockingTask,
        max_retries=max_retries,
        default_retry_delay=default_retry_delay,
        track_started=True,
    )
