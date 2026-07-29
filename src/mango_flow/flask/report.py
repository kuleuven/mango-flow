import celery.states
from celery.result import AsyncResult
from flask import Blueprint, make_response, render_template

from ..lib.report import TaskProgressPart
from ..mango_flow_app import mango_flow

MF_UNREADY_STATES = set(celery.states.UNREADY_STATES)
MF_UNREADY_STATES.add(
    "PROGRESS"
)  # {'PROGRESS', 'RETRY', 'STARTED', 'RECEIVED', 'REJECTED', 'PENDING'}

mango_flow_report_bp = Blueprint(
    "mango_flow_report_bp",
    __name__,
    template_folder="templates",
    static_folder="static/mango_flow",
    static_url_path="/static/mango_flow",
)


def get_task_result(task_id: str) -> dict:
    task_async_result = AsyncResult(id=task_id, app=mango_flow)
    result = {
        "ready": task_async_result.ready(),
        "successful": task_async_result.successful(),
        "children": task_async_result.children if task_async_result.ready() else None,
        "value": task_async_result.result if task_async_result.ready() else None,
        "parents": task_async_result.as_list(),
    }
    return result


@mango_flow_report_bp.route("/mango_flow/task/report/<task_id>")
def task_report(task_id):
    result = AsyncResult(id=task_id, app=mango_flow)
    return render_template(
        "mango_flow/task_status.html.j2",
        result=get_task_result(task_id),
        raw_result=result,
    )


@mango_flow_report_bp.route("/mango_flow/multi_task/report/<task_ids>")
def multi_task_report(task_ids):
    task_ids = task_ids.split(",")
    raw_results = []
    for task_id in task_ids:
        raw_results.append(AsyncResult(id=task_id, app=mango_flow))

    return render_template(
        "mango_flow/admin/multi_task_status-htmx.html.j2",
        raw_results=raw_results,
        task_ids=",".join(task_ids),
    )


@mango_flow_report_bp.route("/mango_flow/report/multitask/htmx/<task_ids>")
def multi_task_report_htmx(task_ids):
    task_ids = task_ids.split(",")
    raw_results = []
    for task_id in task_ids:
        raw_result = AsyncResult(id=task_id, app=mango_flow)
        if raw_result.info and isinstance(raw_result.info, dict) and raw_result.info.get("progress_items", None):
            # convert them to the richer dataclass version from the saved dict
            print(f"Converting result from task {raw_result.name}")
            progress_items = [
                TaskProgressPart(**progress_item)
                for progress_item in raw_result.info["progress_items"]
            ]
            setattr(raw_result, "progress_items", progress_items)
            print(f"{raw_result.progress_items=}")
        raw_results.append(raw_result)
    

    response = make_response(
        render_template(
            "mango_flow/admin/partials/flow_progress.html.j2", raw_results=raw_results
        )
    )
    task_states = set([raw_result.state for raw_result in raw_results])
    if not (task_states & MF_UNREADY_STATES):
        # no more tasks in any kind of execution mode, ask not to be requested anymore
        # from htmx requests
        response.status_code = 286
    return response
