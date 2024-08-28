from celery import shared_task, current_app, Celery, signature
from celery.canvas import group, chain, chunks
from .irods_watcher import get_modified_data_object_paths

# from .extract_metadata import wrap_extract_metadata_from_path
from plugins.operator import get_zone_operator_session
from irods.meta import iRODSMeta
import pathlib
import datetime
import zoneinfo
import hashlib

from .import MangoFlowTask

@shared_task(name="mango_flow.monitor.irods.subtree", bind=True, base=MangoFlowTask)
def monitor_subtree(self, subtree: str, wf_action: str, *args, **kwargs):
    path = pathlib.Path(subtree)
    zone = path.parts[1]
    client_user = kwargs.pop("client_user", None)
    path_input_mode =  kwargs.pop("path_input_mode", "single")
    try:
        irods_session = get_zone_operator_session(zone, client_user=client_user)
    except Exception as e:
        return str(e)

    subtree_as_collection = irods_session.collections.get(subtree)
    wf_action_hash = hashlib.md5(wf_action.encode()).hexdigest()
    previous_run_marker_name = f"mf.monitor_subtree.{wf_action_hash}"
    previous_run_marker_unit = f"{wf_action}"
    try:
        previous_run_marker = subtree_as_collection.metadata.get_one(
            previous_run_marker_name
        )
        previous_run_marker = datetime.datetime.fromisoformat(previous_run_marker.value)
    except:
        previous_run_marker = None
    admin = False if client_user else True
    subtree_as_collection.metadata(admin=admin).set(
        iRODSMeta(
            previous_run_marker_name,
            (datetime.datetime.now(tz=zoneinfo.ZoneInfo("UTC")).isoformat()),
            previous_run_marker_unit,
        )
    )

    do_paths = get_modified_data_object_paths(
        irods_session,
        subtree=subtree,
        modified_mode="data_object",
        modified_since=previous_run_marker,
    )
    do_paths += get_modified_data_object_paths(
        irods_session,
        subtree=subtree,
        modified_mode="metadata",
        modified_since=previous_run_marker,
    )

    do_paths = set(do_paths) # remove duplicates
    app : Celery = current_app
    if path_input_mode == "single":
        for do_path in do_paths:
            args = [do_path]
            task : signature = app.signature(wf_action, args, kwargs['kwargs'])
            task.apply_async()

    if path_input_mode == "multi":
        args = do_paths
        task : signature = app.signature(wf_action, args, kwargs['kwargs'])
        task.apply_async()



    return f"Processed {len(do_paths)}, previous run was at {previous_run_marker}"
