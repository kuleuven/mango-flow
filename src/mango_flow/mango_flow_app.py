# -*- coding: utf-8 -*-
# This app should be launched from the Mango Portal project root, 
# it contains the ManGO Flow Celery app

import logging
import os
from importlib import import_module

from celery import Celery, chord, shared_task
from celery.canvas import chain, signature

from .bootstrap_config import get_beat_configs_from_global_file
from .mango_flow_workers_config import MANGO_FLOW_MODULES


class MFException(Exception):
    def __init__(self, message="Mango Flow error", **params):
        super().__init__(f"{message} : {params}")


logger = logging.getLogger(__name__)

broker = os.environ.get("MANGO_FLOW_BROKER", "redis://localhost:6379/0")
backend = os.environ.get("MANGO_FLOW_BACKEND", "redis://localhost:6379/1")

for task_group in MANGO_FLOW_MODULES:
    # @todo: read the default package name from elsewhere, as it may be an installed package
    module = import_module(
        task_group["module"], package=task_group.get("package", "plugins.mango_flow")
    )


@shared_task(name="mango_flow.heartbeat")
def mango_flow_heartbeat() -> str:
    return "All is fine in Andromeda"


@shared_task(name="mango_flow.schedule.heartbeat")
def mango_flow_schedule_heartbeat() -> str:
    return "All is fine"


@shared_task(name="mango_flow.irods.chain.path", bind=True)
def mango_flow_chain_path_tasks(self, path: str, tasks: list):
    """"""
    initial_task_dict = tasks.pop(0)
    # the entrypoint of the chain includes the (initial) path as the first argument
    initial_task = signature(
        initial_task_dict["name"], args=[path], kwargs=initial_task_dict["parameters"]
    )
    task_chain = chain(
        initial_task,
        *[signature(task["name"], kwargs=task["parameters"]) for task in tasks],
    )

    # for task in tasks:
    #     # the rest are signature partials, the output of the first task will be injected as the first arg in the next
    #    remainig_tasks.addsignature(task["name"], kwargs=task["parameters"]))

    return task_chain.apply_async()


@shared_task(name="mango_flow.irods.chord.chain.path", bind=True)
def mango_flow_chord_chain(
    self, paths: list, header_tasks: list = [], aggregate_tasks: list = []
):
    """
    Multi-input, every input path processed In parallel. The results are processed by the aggregate task
    """

    if not paths or not header_tasks or not aggregate_tasks:
        raise (MFException(input_parameters="missing items in parameters"))

    chord_header_tasks = []

    for path in paths:
        header_signature = signature(
            header_tasks[0]["name"], args=[path], kwargs=header_tasks[0]["parameters"]
        )
        for header_task in header_tasks[1:]:
            new_signature = signature(
                header_task["name"], kwargs=header_task["parameters"]
            )
            header_signature.link(new_signature)
        chord_header_tasks.append(header_signature)

    aggregate_signature = signature(
        aggregate_tasks[0]["name"], kwargs=aggregate_tasks[0]["parameters"]
    )
    for aggregate_task in aggregate_tasks[1:]:
        aggregate_signature.link(
            signature(aggregate_task["name"], kwargs=aggregate_task["parameters"])
        )

    result = chord(chord_header_tasks, aggregate_signature).apply_async()
    logger.info(
        f"Chord chain completed for header actions {[head['name'] for head in header_tasks]} and aggregators {[agg['name'] for agg in aggregate_tasks] }"
    )
    return result


@shared_task(name="mango_flow.tasks.noop", bind=True)
def mango_flow_noop(self, *args, **kwargs):
    """
    No operation task debugging.
    """
    logger.info(f"mango_flow_noop: {args=}, {kwargs=}")
    return None


beat_schedule = {
    "heartbeat-300-seconds": {
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
    worker_concurrency=2,
    beat_max_loop_interval=10,  # seconds
    beat_schedule=beat_schedule,
)

# specific for the redbeat scheduler
mango_flow.conf.redbeat_redis_options = {
    "retry_period": -1,
}

mango_flow.conf.beat_schedule = beat_schedule
