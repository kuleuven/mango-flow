import celery
import irods.exception
import jinja2
import mango_lib.irods.auth
from mango_lib.jinja import MangoJinjaExtension


class MangoFlowTask(celery.Task):
    """Custom implementation of a celery Task class"""

    auto_retry_for = (irods.exception.CAT_CONNECT_ERR, irods.exception.NetworkException)
    retry_backoff = True  # exponential backoff
    retry_backoff_max = 1800  # 30 min is the maximum to wait between retries, but it will try for much longer in total
    # retry_jitter = True  # default, avoid thermonuclear reactions


# ManGO Flow wide operator configuration
# fetched from ManGO Lib which in turn looks also for an ENV variable
# to specify the specific implementation

get_zone_operator_session = mango_lib.irods.auth.get_zone_operator_session


# ManGO Flow wide jinja2 environment
# This environment is used for generating all kinds of strings using the included filters and
# some extra ManGO/iRODS for example to obtain metadata for a certain irods item .

MANGO_FLOW_JINJA2_ENVIRONMENT = jinja2.Environment(
    autoescape=jinja2.select_autoescape(default=False, default_for_string=False),
    trim_blocks=True,
    lstrip_blocks=True,
    extensions=[MangoJinjaExtension],
)


# utility function that uses the dedicated Jinja2 environment to apply a template with the given variables
def mf_apply_jinja2_template(dest_template: str, **variables):
    return MANGO_FLOW_JINJA2_ENVIRONMENT.from_string(dest_template).render(**variables)

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
        task_group["module"], package=task_group.get("package", "mango_flow")
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
        f"Chord chain completed for header actions {[head['name'] for head in header_tasks]} and aggregators {[agg['name'] for agg in aggregate_tasks]}"
    )
    return result


@shared_task(name="mango_flow.tasks.noop", bind=True)
def mango_flow_noop(self, *args, **kwargs):
    """
    No operation task debugging.
    """
    logger.info(f"mango_flow_noop: {args=}, {kwargs=}")


def get_celery_app() -> celery.Celery:
    beat_schedule = {
        "heartbeat-300-seconds": {
            "task": "mango_flow.schedule.heartbeat",
            "schedule": 3600.0,
        }
    }

    mango_global_config_file_name = os.environ.get("MANGO_FLOW_GLOBAL_CONFIG", None)

    if mango_global_config_file_name:
        beat_schedule |= get_beat_configs_from_global_file(mango_global_config_file_name)

    task_routes = {}
    task_routes_file = os.environ.get("MANGO_FLOW_TASK_ROUTES", None)
    if task_routes_file and os.path.exists(task_routes_file):
        import yaml

        with open(task_routes_file, "r") as f:
            task_routes = yaml.safe_load(f)
            # Should validate here the structure

        logging.info(f"Loaded task_routes from {task_routes_file}: {task_routes}")


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
        result_extended=True,
        task_track_started=True,
        worker_send_task_events=True,
        task_send_sent_event=True,
        task_routes=task_routes,
    )

    # specific for the redbeat scheduler
    mango_flow.conf.update(redbeat_redis_options={
        "retry_period": -1,
    })

    mango_flow.conf.beat_schedule = beat_schedule

    return mango_flow

app = get_celery_app()