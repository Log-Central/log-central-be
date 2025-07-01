from monitoring.domain.i_repo.i_visualization_platform_repo.i_dashbaord_repo import (
    IDashboardRepo,
    IPublicDashboardRepo,
)
from monitoring.domain.visualization_platform.dashboard import (
    Dashboard,
    PublicDashboard,
)
from monitoring.infra.models.visualization_platform_model import (
    DashboardModel,
    PublicDashboardModel,
)


class DashboardRepo(IDashboardRepo):
    def save(self, dashboard: Dashboard) -> None:
        """
        Dashboard 도메인 객체를 DB에 저장하거나 업데이트합니다.
        """
        DashboardModel.objects.update_or_create(
            id=dashboard.id, defaults=dashboard.to_dict()
        )

    def find_by_uid(self, uid: str) -> Dashboard | None:
        """
        플랫폼 내부 UID로 Dashboard를 조회하여 도메인 객체로 반환합니다.
        """
        try:
            model = DashboardModel.objects.get(uid=uid)
            return Dashboard(
                id=model.id,
                uid=model.uid,
                title=model.title,
                user_id=model.user_id,
                org_id=model.org_id,
                project_id=model.project_id,
                folder_uid=model.folder_uid,
                url=model.url,
                config_json=model.config_json,
                panels=model.panels,
                tags=model.tags,
                data_sources=model.data_sources,
            )
        except DashboardModel.DoesNotExist:
            return None

    def find_by_user_id(self, user_id: str) -> Dashboard | None:
        """
        사용자 ID로 Dashboard를 조회하여 도메인 객체로 반환합니다.
        """
        try:
            model = DashboardModel.objects.get(user_id=user_id)
            return Dashboard(
                id=model.id,
                uid=model.uid,
                title=model.title,
                user_id=model.user_id,
                org_id=model.org_id,
                project_id=model.project_id,
                folder_uid=model.folder_uid,
                url=model.url,
                config_json=model.config_json,
                panels=model.panels,
                tags=model.tags,
                data_sources=model.data_sources,
            )
        except DashboardModel.DoesNotExist:
            return None

    def find_by_project_id(self, project_id: str) -> Dashboard | None:
        """
        프로젝트 ID로 Dashboard를 조회하여 도메인 객체로 반환합니다.
        """
        try:
            model = DashboardModel.objects.get(project_id=project_id)
            return Dashboard(
                id=model.id,
                uid=model.uid,
                title=model.title,
                user_id=model.user_id,
                org_id=model.org_id,
                project_id=model.project_id,
                folder_uid=model.folder_uid,
                url=model.url,
                config_json=model.config_json,
                panels=model.panels,
                tags=model.tags,
                data_sources=model.data_sources,
            )
        except DashboardModel.DoesNotExist:
            return None

    def update_url(self, dashboard_id: str, url: str) -> None:
        """
        주어진 Dashboard ID의 URL을 업데이트합니다.
        """
        DashboardModel.objects.filter(id=dashboard_id).update(url=url)


class PublicDashboardRepo(IPublicDashboardRepo):
    def save(self, public_dashboard: PublicDashboard) -> None:
        """
        PublicDashboard 도메인 객체를 DB에 저장하거나 업데이트합니다.
        """

        PublicDashboardModel.objects.update_or_create(
            id=public_dashboard.id,
            defaults={
                "uid": public_dashboard.uid,
                "dashboard_id": public_dashboard.dashboard_id,
                "project_id": public_dashboard.project_id,
                "public_url": public_dashboard.public_url,
            },
        )

    def find_by_project_id(self, project_id: str) -> PublicDashboard | None:
        """
        프로젝트 ID로 PublicDashboard를 조회하여 도메인 객체로 반환합니다.
        """
        try:
            model = PublicDashboardModel.objects.get(project_id=project_id)
            return PublicDashboard(
                id=model.id,
                uid=model.uid,
                dashboard_id=model.dashboard.id,
                project_id=model.project_id,
                public_url=model.public_url,
            )
        except PublicDashboardModel.DoesNotExist:
            return None
