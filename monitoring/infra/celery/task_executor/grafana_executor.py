from typing import Any

from celery import chain
from typing_extensions import override

from monitoring.domain.visualization_platform import dashboard
from monitoring.infra.celery.tasks.grafana_tasks import (
    task_create_grafana_dashboard,
    task_create_grafana_folder,
    task_create_grafana_public_dashboard,
    task_create_grafana_service_account,
    task_create_grafana_service_token,
    task_get_grafana_dashboard,
    task_get_grafana_folders,
    task_set_grafana_folder_permissions,
)
from monitoring.infra.celery.tasks.monitoring_project_tasks import (
    finalize_monitoring_project,
    handle_monitoring_project_failure,
)
from monitoring.service.i_executors.excutor_DTO import (
    BaseProvisionDTO,
    CreateDashboardDTO,
    CreatePublicDashboardDTO,
    CreateServiceAccountDTO,
    CreateServiceTokenDTO,
    CreateUserFolderDTO,
    FinalizeDashboardDTO,
    ProvisionFailureDTO,
    SetFolderPermissionsDTO,
)
from monitoring.service.i_executors.visualization_platform_executor import (
    VisualizationPlatformTaskExecutor,
)


class GrafanaTaskExecutor(VisualizationPlatformTaskExecutor):
    # ─────────────────────────────────────────────────────────────────────────────
    # Signature generators
    # ─────────────────────────────────────────────────────────────────────────────
    def get_create_user_folder_sig(self, task_id: str, user_id: str, folder_name: str):
        return task_create_grafana_folder.si(task_id, user_id, folder_name).set(
            task_id=task_id
        )

    def get_create_service_account_sig(
        self,
        task_id: str,
        project_id: str,
        account_name: str,
        user_id: str,
        role: str = "Viewer",
    ):
        return task_create_grafana_service_account.si(
            task_id, project_id, account_name, user_id, role
        ).set(task_id=task_id)

    def get_create_service_token_sig(
        self, task_id: str, project_id: str, token_name: str
    ):
        return task_create_grafana_service_token.si(
            task_id, project_id, token_name
        ).set(task_id=task_id)

    def get_set_folder_permissions_sig(
        self, task_id: str, user_id: str, project_id: str
    ):
        return task_set_grafana_folder_permissions.si(task_id, user_id, project_id).set(
            task_id=task_id
        )

    def get_create_dashboard_sig(
        self,
        task_id: str,
        user_id: str,
        project_id: str,
        dashboard_title: str,
        dashboard_config: dict,
    ):
        return task_create_grafana_dashboard.si(
            task_id, user_id, project_id, dashboard_title, dashboard_config
        ).set(task_id=task_id)

    def get_create_public_dashboard_sig(self, task_id: str, project_id: str):
        return task_create_grafana_public_dashboard.si(task_id, project_id).set(
            task_id=task_id
        )

    def get_finalize_monitoring_project_sig(
        self, task_id: str, user_id: str, project_id: str
    ):
        return finalize_monitoring_project.si(task_id, user_id, project_id).set(
            task_id=task_id
        )

    def get_get_dashboard_by_uid_sig(self, task_id: str, dashboard_uid: str):
        return task_get_grafana_dashboard.si(task_id, dashboard_uid).set(
            task_id=task_id
        )

    def get_get_grafana_folders_sig(self, task_id: str):
        return task_get_grafana_folders.si(task_id).set(task_id=task_id)

    def build_base_resources_chain(
        self,
        dto: BaseProvisionDTO,
    ):
        """
        유저 폴더 생성 → 서비스 계정 생성 → 토큰 생성(wrapper) → 권한 설정(wrapper)
        """
        return chain(
            self.get_create_user_folder_sig(
                task_id=dto.folder_task_id,
                user_id=dto.user.id,
                folder_name=dto.folder_name,
            ),
            self.get_create_service_account_sig(
                task_id=dto.account_task_id,
                project_id=dto.project_id,
                account_name=dto.account_name,
                user_id=dto.user.id,
            ),
            self.get_create_service_token_sig(
                task_id=dto.token_task_id,
                project_id=dto.project_id,
                token_name=dto.token_name,
            ),
            self.get_set_folder_permissions_sig(
                task_id=dto.perm_task_id,
                user_id=dto.user.id,
                project_id=dto.project_id,
            ),
        )

    def build_dashboard_provision_chain(
        self,
        *,
        user_folder: CreateUserFolderDTO | None = None,
        service_account: CreateServiceAccountDTO | None = None,
        service_token: CreateServiceTokenDTO | None = None,
        permissions: SetFolderPermissionsDTO | None = None,
        dashboard: CreateDashboardDTO | None = None,
        public_dashboard: CreatePublicDashboardDTO | None = None,
        finalize_dashboard: FinalizeDashboardDTO | None = None,
    ) -> Any | None:
        """
        - 모든 DTO는 None 허용
        - 제공된 DTO만 순서대로 체인에 포함
        - chain(*sigs)로 Canvas 객체를 반환, DTO가 하나도 없으면 None 반환
        """
        sigs = []

        if user_folder:
            sigs.append(
                self.get_create_user_folder_sig(
                    task_id=user_folder.task_id,
                    user_id=user_folder.user_id,
                    folder_name=user_folder.folder_name,
                )
            )

        if service_account:
            sigs.append(
                self.get_create_service_account_sig(
                    task_id=service_account.task_id,
                    project_id=service_account.project_id,
                    account_name=service_account.account_name,
                    user_id=service_account.user_id,
                )
            )

        if service_token:
            sigs.append(
                self.get_create_service_token_sig(
                    task_id=service_token.task_id,
                    project_id=service_token.project_id,
                    token_name=service_token.token_name,
                )
            )

        if permissions:
            sigs.append(
                self.get_set_folder_permissions_sig(
                    task_id=permissions.task_id,
                    user_id=permissions.user_id,
                    project_id=permissions.project_id,
                )
            )

        if dashboard:
            sigs.append(
                self.get_create_dashboard_sig(
                    task_id=dashboard.task_id,
                    user_id=dashboard.user_id,
                    project_id=dashboard.project_id,
                    dashboard_title=dashboard.dashboard_title,
                    dashboard_config=dashboard.dashboard_config,
                )
            )

        if public_dashboard:
            sigs.append(
                self.get_create_public_dashboard_sig(
                    task_id=public_dashboard.task_id,
                    project_id=public_dashboard.project_id,
                )
            )

        if finalize_dashboard:
            sigs.append(
                self.get_finalize_monitoring_project_sig(
                    task_id=finalize_dashboard.task_id,
                    user_id=finalize_dashboard.user_id,
                    project_id=finalize_dashboard.project_id,
                )
            )

        return chain(*sigs) if sigs else None

    # ─────────────────────────────────────────────────────────────────────────────
    # Provision chain
    # ─────────────────────────────────────────────────────────────────────────────

    @override
    def dispatch_provision_dashboard_workflow(
        self,
        *,
        user_folder: CreateUserFolderDTO | None = None,
        service_account: CreateServiceAccountDTO | None = None,
        service_token: CreateServiceTokenDTO | None = None,
        permissions: SetFolderPermissionsDTO | None = None,
        dashboard: CreateDashboardDTO | None = None,
        public_dashboard: CreatePublicDashboardDTO | None = None,
        finalize_dashboard: FinalizeDashboardDTO | None = None,
        failure: ProvisionFailureDTO,
    ) -> str:
        """
        - build_dashboard_provision_chain와 동일한 인자를 키워드로 받음
        - failure DTO는 에러 핸들러에 쓰일 task_id, project_id 포함
        - 반환값: failure.project_id (모니터링 프로젝트 ID)
        """
        chain_sig = self.build_dashboard_provision_chain(
            user_folder=user_folder,
            service_account=service_account,
            service_token=service_token,
            permissions=permissions,
            dashboard=dashboard,
            public_dashboard=public_dashboard,
            finalize_dashboard=finalize_dashboard,
        )

        if chain_sig:
            chain_sig.apply_async(
                link_error=handle_monitoring_project_failure.si(
                    failure.task_id,
                    failure.project_id,
                )
            )

        return failure.project_id
