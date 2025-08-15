import os
import time
from monitoring.infra.celery.tasks.utils import locking_task


@locking_task(max_retries=3, default_retry_delay=2)
def long_task(self, task_id: str):
    print(f"[{os.getpid()}] TEST TASK 시작합니다!! 60초동안 기다립니다")
    time.sleep(60)
    print(f"[{os.getpid()}] TEST TASK 종료되었습니다.")
