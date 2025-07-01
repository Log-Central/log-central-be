from typing import Any

from typing_extensions import override

from monitoring.domain.i_repo.i_task_result_repo import ITaskResultRepo
from monitoring.domain.task_result import TaskResult, TaskStatus
from monitoring.infra.models.task_result_model import TaskResultModel


class TaskResultRepo(ITaskResultRepo):
    @override
    def find_by_task_id(self, task_id: str) -> TaskResult | None:
        try:
            model = TaskResultModel.objects.get(id=task_id)
            return TaskResult(
                id=model.id,
                task_name=model.task_name,
                status=TaskStatus(model.status),
                result=model.result,
                date_created=model.date_created.isoformat(),
                date_started=(
                    model.date_started.isoformat() if model.date_started else None
                ),
                date_done=model.date_done.isoformat() if model.date_done else None,
                traceback=model.traceback,
                retries=model.retries,
            )
        except TaskResultModel.DoesNotExist:
            return None

    @override
    def save(self, task_result: TaskResult) -> None:
        TaskResultModel.objects.update_or_create(
            id=task_result.id, defaults=task_result.to_dict()
        )

    @override
    def update(self, task_result: TaskResult) -> None:
        TaskResultModel.objects.filter(id=task_result.id).update(
            **task_result.to_dict()
        )

    @override
    def update_fields(self, task_id: str, **fields: Any) -> None:
        TaskResultModel.objects.filter(id=task_id).update(**fields)

    @override
    def bulk_upsert(self, task_results: list[TaskResult]) -> None:
        """
        Django 4.1+ bulk_create의 update_conflicts 옵션을 활용한 upsert 예시.
        존재하지 않으면 insert, 존재하면 update 합니다.
        """
        models = [
            TaskResultModel(
                id=tr.id,
                task_name=tr.task_name,
                status=tr.status.value,
                result=tr.result,
                date_created=tr.date_created,
                date_started=tr.date_started,
                date_done=tr.date_done,
                traceback=tr.traceback,
                retries=tr.retries,
            )
            for tr in task_results
        ]
        # 단일 쿼리로 insert 실패 시 update 수행
        TaskResultModel.objects.bulk_create(
            models,
            update_conflicts=True,
            unique_fields=["id"],
            update_fields=[
                "task_name",
                "status",
                "result",
                "date_created",
                "date_started",
                "date_done",
                "traceback",
                "retries",
            ],
        )
