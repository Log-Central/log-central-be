from django.urls import path

from monitoring.interface.views.log_monitoring_project_views import (
    LogMonitoringProjectStep1View,
    LogMonitoringProjectStep2View,
)
from monitoring.interface.views.my_monitoring_project_views import (
    MyMonitoringProjectView,
)
from monitoring.interface.views.my_monitoring_projects_view import (
    MyMonitoringProjectsView,
)
from monitoring.interface.views.test_kill_worker import TestKillWorkerView

urlpatterns = [
    path(
        "monitoring/log-project/step1",
        LogMonitoringProjectStep1View.as_view(),
        name="logProjectStep1",
    ),
    path(
        "monitoring/log-project/step2",
        LogMonitoringProjectStep2View.as_view(),
        name="logProjectStep2",
    ),
    path(  # 내 모니터링 프로젝트 목록
        "monitoring/project/<str:project_id>/",
        MyMonitoringProjectView.as_view(),
        name="monitoring-project-detail",
    ),
    path(
        "monitoring/projects/",
        MyMonitoringProjectsView.as_view(),
        name="monitoring-projects-list",
    ),
    path("test-kill-worker/", TestKillWorkerView.as_view(), name="test-kill-worker"),
]
