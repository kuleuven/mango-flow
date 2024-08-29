# This app should be launched from the Mango Portal project root

from celery import Celery, shared_task, xmap
from celery.canvas import chain, signature
from importlib import import_module
from plugins.mango_flow.bootstrap_config import get_beat_configs_from_global_file

import logging
import os


class MFException(Exception):
    def __init__(self, message="Mango Flow error", **params):
        super().__init__(f"{message} : {params}")


broker = os.environ.get("MANGO_FLOW_BROKER", "redis://localhost:6379/0")
backend = os.environ.get("MANGO_FLOW_BACKEND", "redis://localhost:6379/1")

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
    {
        "module": "plugins.mango_flow.tasks.reporting",
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


@shared_task(name="mango_flow.irods.chain.path", bind=True)
def mango_flow_chain_path_tasks(self, path: str, tasks: list):
    """"""
    start_task = tasks.pop(0)
    # the entrypoint of the chain includes the (initial) path as the first argument
    task_chain = signature(
        start_task["name"], args=[path], kwargs=start_task["parameters"]
    )
    for task in tasks:
        # the rest are signature partials, the output of the first task will be injected as the first arg in the next
        task_chain.link(signature(task["name"], kwargs=task["parameters"]))

    task_chain.apply_async()


@shared_task(name="mango_flow.irods.map.chain.path", bind=True)
def mango_flow_map_chain(
    self, paths: list, header_tasks: list = [], aggregate_tasks: list = []
):
    """
    Multi-input, but every input path processed sequentially. The results are processed by the aggregate task
    """
    # print(f"header_tasks: {header_tasks}")
    # print(f"aggregator tasks : {aggregate_tasks}")
    if not paths or not header_tasks or not aggregate_tasks:
        raise (MFException(input_parameters="missing items in parameters"))

    header_signature = signature(
        header_tasks[0]["name"], kwargs=header_tasks[0]["parameters"]
    )
    for header_task in header_tasks[1:]:
        header_signature.link(header_task["name"], kwargs=header_task["parameters"])

    aggregate_signature = signature(
        aggregate_tasks[0]["name"], kwargs=aggregate_tasks[0]["parameters"]
    )
    for aggregate_task in aggregate_tasks[1:]:
        aggregate_signature.link(
            aggregate_task["name"], kwargs=aggregate_task["parameters"]
        )
    # PROBLEM TO BE SOLVED: the kwargs in the signatures inside xmap are thrown away
    # the aggregate_signature has them
    result = chain(
        xmap(
            header_signature,
            paths,
        ),
        aggregate_signature,
    ).apply_async()


beat_schedule = {
    "hearbeat-120-seconds": {
        "task": "mango_flow.schedule.heartbeat",
        "schedule": 300.0,
    }
}

mango_global_config_file_name = os.environ.get("MANGO_FLOW_GLOBAL_CONFIG", None)

if mango_global_config_file_name:
    beat_schedule |= get_beat_configs_from_global_file(mango_global_config_file_name)


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
    beat_schedule=beat_schedule,
)

# specific for the redbeat scheduler
mango_flow.conf.redbeat_redis_options = {
    "retry_period": -1,
}

mango_flow.conf.beat_schedule = beat_schedule
