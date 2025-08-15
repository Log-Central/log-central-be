import os

from celery import Celery
from celery.signals import worker_process_init
from kombu import Exchange, Queue

# ─ Django 설정 등록 및 setup ─
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

# ─ Celery 앱 생성 ─
app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks(["monitoring.infra.celery.tasks"])

TASK_QUEUE = "worker_tasks_queue"
ROUTING_KEY = "tasks"
DIRECT_EXCHANGE = "worker_tasks_exchange"

app.conf.worker_send_task_events = True
app.conf.task_track_started = True

worker_queue = Queue(
    TASK_QUEUE,
    Exchange(DIRECT_EXCHANGE, type="direct"),
    routing_key="tasks",
    queue_arguments={"x-queue-type": "quorum"},
    no_declare=True,
)

app.conf.task_queues = [worker_queue]
app.conf.task_create_missing_queues = False

app.conf.task_queues = [
    Queue(
        TASK_QUEUE,
        Exchange(DIRECT_EXCHANGE, type="direct", durable=True),
        routing_key=ROUTING_KEY,
        queue_arguments={"x-queue-type": "quorum"},
    ),
    Queue("celery", Exchange("celery", type="direct"), routing_key="celery"),
]

# broker로부터 ack를 받아야 넘어감
app.conf.broker_transport_options = {"confirm_publish": True}


# ── 시그널 등록 ───────────────────────────────────────────
@worker_process_init.connect
def init_worker(**kwargs):
    print("registering signals")
    import monitoring.infra.celery.tasks.signals.task_signals


# setup_celery_signals()
print("Celery is ready")
