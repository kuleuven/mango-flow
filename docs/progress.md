# Tracking progress (WIP)

Tracking and visualizing the progress of a task requires several steps:

1. The task's ID has to be registered in the iRODS item it is related to,
so that its status can be queried for reporting.

For example [mf_test_tar()](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/admin.py#L255-L263) registers the ID of the task in the metadata of the collection to be tarred.

2. As it runs, the task has to report its status to the redis database.

For example, a [`create_frigo_tar_archive()`](https://gitea.icts.kuleuven.be/foz/cold-storage/src/commit/b60897330a94a4febac95268eecc796c05ca0313/src/cold_storage/tasks/frigo.py#L118-L328),
which uses the [`CheckPointTar` class](./mango_tar.md#the-checkpointtar-class),
registers [a guard to register its status](https://gitea.icts.kuleuven.be/foz/cold-storage/src/commit/b60897330a94a4febac95268eecc796c05ca0313/src/cold_storage/tasks/frigo.py#L107-L115). The guard calls the `update_state()` method of the Celery task.

An alternative (in development) would be to use
[the dedicated dataclass `TaskProgressPart`](#a-dataclass-for-keeping-track-of-progress)
to register and compute progress information;
a `CheckPointTar` instance could initialize this by default
at the individual file level and to count both files and bytes for a tar,
and update the values with each milestone (uploaded chunks or files).


3. A route has to render the report based on the ID of the task(s).

In the case of [mf_test_tar()](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/admin.py#L265-L270), the route redirects to [another route](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/report.py#L42-L52) that renders [a template](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/templates/mango_flow/admin/multi_task_status-htmx.html.j2) fully dedicated to reporting on the progress of the task.

This need not be the case, as [using htmx](#using-htmx-for-the-progress-report)
lets us inject HTML fragments filled in with data from routes into any jinja template.

## Using htmx for the progress report

The [htmx library](https://htmx.org/) lets you make HTTP requests
from arbitrary HTML elements, with arbitrary triggers,
and use the result to replace concrete elements without refreshing the full page.

Let's look for example at the contents of [multi_task_status-htmx.html.j2](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/templates/mango_flow/admin/multi_task_status-htmx.html.j2),
rendered by the [`multi_task_report()` route](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/report.py#L42-L52).

1. The `<script>` tags imports htmx itself.
2. The top-level `<div>` makes an HTTP request to the route specified as the `hx-get` attribute, triggered accordingly to `hx-trigger` (every 2 seconds), and replaces the contents of the element selected by `hx-target` (via a CSS Selector) with the response.

```html
<script src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.7/dist/htmx.min.js"
        integrity="sha384-ZBXiYtYQ6hJ2Y0ZNoYuI+Nq5MqWBr+chMrS/RkXpNzQCApHEhOt2aY8EJgqwHLkJ"
        crossorigin="anonymous"></script>
<div hx-get="/mango_flow/report/multitask/htmx/{{ task_ids }}"
    hx-trigger="load, every 2s"
    hx-target="#result-target">
    <div id="result-target">
        <p>Please wait ... loading</p>
    </div>
</div>
```

Initially, the content of the target element (`"#result-target"` selects an element with the ID "result-target") is a `<p>` element with the text "Please wait... loading". Every two seconds, it will be replaced with the output of [`multi_task_report_htmx()`](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/report.py#L56-L70), the route called by the URL in `hx-get` (this will be replaced with something like `url_for("mango_flow_report_bp.multi_task_report_htmx", task_idx=task_ids)`).

Let's look at that route. Based on the IDs of the tasks to report on,
it uses `celery.result.AsyncResult` to obtain information on the tasks,
including the status reported via the guards.
The list of task results is then sent to [another jinja template](https://gitea.icts.kuleuven.be/foz/mango-flow/src/commit/9d96ed8de3ca09bddeb2732c831899e9b62851bc/src/mango_flow/flask/templates/mango_flow/admin/partials/flow_progress.html.j2) to generate an HTML fragment
with progress bar and any other visualization of the progress information.

Instead of rendering the generated HTML, the route turns it into a response
and returns the response to the div that requested it via htmx.
This way, the rendered template is injected into the `<div id="result-target">`,
effectively replacing the old `<p>` element; every 2 seconds, it repeats.

```python
@mango_flow_report_bp.route("/mango_flow/report/multitask/htmx/<task_ids>")
def multi_task_report_htmx(task_ids):
    task_ids = task_ids.split(",")
    raw_results = []
    for task_id in task_ids:
        raw_results.append(AsyncResult(id=task_id, app=mango_flow))

    print(dir(raw_results[0]))
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
```

## A dataclass for keeping track of progress

The [lib/report.py](../src/mango_flow/lib/report.py) module
defines a [dataclass](https://docs.python.org/3/library/dataclasses.html) `TaskProgressPart` with:

- Variables for a progress bar
- Properties to compute stuff such as ETA, speed...
- Customizable prettyprint functions

It is meant to be instantiated from tasks that want to track progress
as well as from the routes that want to send the information to a template.
The goal of the methods is to easily compute some useful information
and print it nicely.

The `to_dict()` method is necessary to JSON-Serialize it,
in order to communicate the contents from the task, via the redis database, to the route that queries it.
