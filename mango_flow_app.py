# This app should be launched from the Mango Portal project root

from celery import Celery, shared_task
from celery.canvas import chain
from importlib import import_module
from plugins.mango_flow.bootstrap_config import get_beat_configs

import logging
import os

# kafka_broker = os.getenv('KAFKA_BROKER', 'confluentkafka://localhost:29092')
# rabbitmq_broker = "amqp://mango_flow:mango_flow@localhost:5672/mango_flow"
broker = os.environ.get("MANGO_FLOW_BROKER", "redis://localhost:6379/0")
backend = os.environ.get("MANGO_FLOW_BACKEND", "redis://localhost:6379/1")
# db_backend = "db+mysql://kafka:kafka@localhost/kafka"

mf_logger = logging.getLogger(__name__)

MANGO_FLOW_MODULES = [
    {
        "module": "plugins.mango_flow.tasks.basic_operations",
    },
    {
        "module": "plugins.mango_flow.tasks.basic_metadata_extraction",
    },
    {
        "module": "plugins.mango_flow.monitors",
    },
]

for task_group in MANGO_FLOW_MODULES:
    module = import_module(task_group["module"])

@shared_task(name="mango_flow.heartbeat")
def mango_flow_heartbeat() -> str:
    return "All is fine in Andromeda"


@shared_task(name="mango_flow.schedule.heartbeat")
def mango_flow_schedule_heartbeat() -> str:
    return "All is fine"

beat_schedule = {
    "hearbeat-120-seconds": {
        "task": "mango_flow.schedule.heartbeat",
        "schedule": 300.0,
    }}

mango_global_config_file_name = os.environ.get('MANGO_FLOW_GLOBAL_CONFIG', None)

if mango_global_config_file_name:
    beat_schedule |= get_beat_configs(mango_global_config_file_name)


mango_flow = Celery(
    main="mango_flow",
    broker=broker,
    backend=backend,
    beat_scheduler="redbeat.RedBeatScheduler",
    timezone="UTC",
    redis_backend_health_check_interval=30,  # seconds
    redis_retry_on_timeout=True,
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=None,
    broker_channel_error_retry=True,
    worker_concurrency=8,
    beat_max_loop_interval=10,  # seconds
    beat_schedule  = beat_schedule
)

# specific for the redbeat scheduler
mango_flow.conf.redbeat_redis_options = {
    "retry_period": -1,
}

mango_flow.conf.beat_schedule = beat_schedule