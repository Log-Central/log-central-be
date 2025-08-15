import json
import time, threading
from uuid import uuid4
from datetime import datetime
import logging
from dataclasses import dataclass
from functools import partial

from rest_framework.views import APIView
from rest_framework.response import Response
from kombu import Connection
from celery.events import EventReceiver
from config.celery import app
from config.settings import CELERY_BROKER_URL

from monitoring.domain.i_repo.i_task_result_repo import ITaskResultRepo
from monitoring.domain.task_result import TaskResult, TaskStatus
from monitoring.infra.repo.task_result_repo import TaskResultRepo

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

TASK_NAME = "monitoring.infra.celery.tasks.for_test_task.long_task"
TIMEOUT = 180
POLL_SEC = 1


@dataclass
class WorkerKillState:
    first_pid: int | None = None
    first_hostname: str | None = None
    verified: bool = False  # 2→1 감지 여부
    baseline_total: int | None = None
    last_total: int | None = None


def task_make_pending() -> str:
    task_id = str(uuid4())
    task_result = TaskResult(
        id=task_id,
        task_name="long task",
        status=TaskStatus.PENDING,
        date_created=datetime.now().isoformat(),
    )
    repo: ITaskResultRepo = TaskResultRepo()
    repo.save(task_result)
    return task_id


def _fmt_ev(ev: dict) -> str:
    try:
        return json.dumps(ev, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(ev)


def _resolve_child_pid(hostname: str, task_id: str) -> int | None:
    """해당 호스트에서 실행 중인 task_id의 child(worker) PID 조회."""
    insp = app.control.inspect([hostname])
    # 방법 1: query_task (Celery 5.2+에서 제공)
    try:
        q = insp.query_task(task_id) or {}
        info = q.get(hostname) or {}
        pid = info.get("worker_pid") or info.get("pid")
        if pid:
            return int(pid)
    except Exception:
        pass
    # 방법 2: active() 목록에서 찾기 (보편적)
    try:
        act = insp.active() or {}
        for t in act.get(hostname, []):
            if t.get("id") == task_id:
                pid = t.get("worker_pid") or t.get("pid")
                if pid:
                    return int(pid)
    except Exception:
        pass
    return None


def handle_event(
    ev: dict, *, task_id: str, state: WorkerKillState, log: logging.Logger
) -> None:
    event_type = ev.get("type")
    event_uuid = ev.get("uuid")
    log.info("[EV] type=%s uuid=%s", event_type, event_uuid)
    log.debug("[EV payload] %s", _fmt_ev(ev))

    if event_uuid != task_id:
        log.debug("[SKIP] target=%s got=%s", task_id, event_uuid)
        return

    if event_type == "task-started":
        hostname = ev.get("hostname")
        if state.first_pid is None and hostname:
            # 여기서 child PID를 해석해 기록
            child = _resolve_child_pid(hostname, task_id)
            if child:
                state.first_pid = child
                state.first_hostname = hostname
                log.info("✅ 첫 시작(child): host=%s child_pid=%s", hostname, child)
            else:
                # fallback: 이벤트의 pid(메인 PID)도 기록해 두되, child는 아님을 명시
                log.info(
                    "ℹ️ 이벤트 pid=%s (메인 PID 추정), child PID 조회 실패",
                    ev.get("pid"),
                )

    elif event_type == "task-succeeded":
        log.info("[MATCH] task-succeeded")
    elif event_type == "task-failed":
        log.info("[MATCH] task-failed")
    elif event_type == "task-received":
        log.info("[MATCH] task-received")
    elif event_type == "task-retried":
        log.info("[MATCH] task-retried")


def _get_pool_pids():
    """모든 워커 노드의 child PID 목록과 총합을 반환."""
    insp = app.control.inspect()
    stats = insp.stats() or {}
    node_pids = {}
    total = 0
    for node, s in stats.items():
        pool = s.get("pool") or {}
        pids = pool.get("processes")
        if isinstance(pids, (list, tuple)):
            node_pids[node] = [int(x) for x in pids]
            total += len(pids)
    return total, node_pids


def poll_pool_change(state: WorkerKillState, log: logging.Logger):
    """baseline 수집 후, 2→1 변화가 감지되면 로그 출력하고 state.verified = True."""
    deadline = time.time() + TIMEOUT

    # baseline 확보
    while time.time() < deadline and state.baseline_total is None:
        total, m = _get_pool_pids()
        if total is not None:
            state.baseline_total = total
            state.last_total = total
            log.info("baseline: total=%s, map=%s", total, m)
            break
        time.sleep(POLL_SEC)

    # 변화 감시
    while time.time() < deadline and not state.verified:
        total, m = _get_pool_pids()
        if total != state.last_total:
            log.info("pool change: %s → %s (map=%s)", state.last_total, total, m)
            if state.last_total == 2 and total == 1:
                log.info("✅ 감지: 워커 child 프로세스 수 2→1")
                state.verified = True
                return
            state.last_total = total
        time.sleep(POLL_SEC)


class TestKillWorkerView(APIView):
    def get(self, request):
        broker_url = CELERY_BROKER_URL
        task_id = task_make_pending()
        state = WorkerKillState()

        # 이벤트 송신(워커가 -E면 생략 가능)
        app.control.enable_events()
        logger.info("이벤트 리시버를 먼저 시작합니다.")

        # 1) 이벤트 리시버 스레드
        with Connection(broker_url) as conn:
            recv = EventReceiver(
                conn,
                handlers={
                    "*": partial(
                        handle_event,
                        task_id=task_id,
                        state=state,
                        log=logger,
                    )
                },
            )
            recv_thread = threading.Thread(
                target=lambda: recv.capture(limit=None, timeout=TIMEOUT, wakeup=True),
                daemon=True,
            )
            recv_thread.start()

            # 2) 워커 child 프로세스 개수 감시 스레드(2→1 감지)
            poll_thread = threading.Thread(
                target=poll_pool_change, args=(state, logger), daemon=True
            )
            poll_thread.start()

            time.sleep(1)
            logger.info("태스크 전송 시작")
            res = app.send_task(TASK_NAME, args=[task_id], task_id=task_id)
            if res.id != task_id:
                logger.warning("다른 task_id 반환: %s", res.id)

            # 수동으로 워커 child 하나를 kill한 뒤, 2→1 로그가 찍히는지 확인
            recv_thread.join(timeout=TIMEOUT)
            poll_thread.join(timeout=TIMEOUT)

        result = "success" if state.verified else "fail"
        return Response({"uuid": task_id, "broker": broker_url, "result": result})
