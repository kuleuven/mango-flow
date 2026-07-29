import datetime
import itertools
import operator
import os
import pathlib
import tarfile
import time
import zipfile

from celery import shared_task
from irods.column import Criterion, Like
from irods.data_object import iRODSDataObject
from irods.models import Collection, DataObject
from irods.query import Query
from irods.session import iRODSSession
from mango_lib.irods.auth import get_zone_operator_session

from .. import MangoFlowTask


def get_data_object_paths_in_subtree(
    session: iRODSSession,
    subtree: str,
) -> list:
    """Helper function, returns a list of tuples with path, size, modify time and irods checksum if present"""
    objects = [
        Collection.name,
        DataObject.name,
        DataObject.size,
        DataObject.checksum,
        DataObject.modify_time,
    ]

    filters = []
    # only data objects, also avoid the duplicates in the result
    filters += [Criterion("=", DataObject.replica_number, 0)]
    filters += [Like(Collection.name, f"{subtree}%")]
    batch_size = 1000000
    offset = 0
    base_query = (
        Query(session, *objects).filter(*filters).limit(batch_size).offset(offset)
    )

    # count_result = base_query.count().execute()

    results = []
    # to do: loop in batches if total is larger than batch size
    try:
        query_results = base_query.execute()
        for r in query_results:
            results.append(
                (
                    f"{r[Collection.name]}/{r[DataObject.name]}",
                    r[DataObject.size],
                    r[DataObject.modify_time].timestamp(),
                    r[DataObject.checksum],
                )
            )
    except Exception as e:
        print(e)
        results = []
    return results


def get_collection_paths_in_subtree(
    session: iRODSSession,
    subtree: str,
) -> list:
    """Helper function, returns a list of collection paths inside the subtree"""
    objects = [Collection.name]

    filters = [Like(Collection.name, f"{subtree}%")]

    query = Query(session, *objects).filter(*filters).limit(1000000)
    try:
        results = []
        result = query.execute()
        for r in result:
            results.append(f"{r[Collection.name]}")
    except Exception as e:
        results = []
    return results


def expand_paths(
    irods_session: iRODSSession,
    data_object_paths: list[str] = [],
    collection_paths: list[str] = [],
) -> dict[str, list]:
    """
    helper function, accepts zero or more data object paths and/or zero or more collection paths
    and expands these recursively

    """
    # datetime.datetime.timestamp()
    expanded_result = {"data_objects": [], "collections": [], "stats": {}}
    for data_object_path in data_object_paths:
        data_object: iRODSDataObject = irods_session.data_objects.get(data_object_path)
        expanded_result["data_objects"].append(
            (
                data_object.path,
                data_object.size,
                data_object.modify_time.timestamp(),
                data_object.checksum,
            )
        )

    for collection in collection_paths:
        expanded_result["data_objects"] += get_data_object_paths_in_subtree(
            irods_session, subtree=collection
        )
        expanded_result["collections"] += get_collection_paths_in_subtree(
            irods_session, subtree=collection
        )
    # expanded

    # inefficient, should be done earlier while looping over query results:
    expanded_result["stats"] = {
        "num_collections": len(expanded_result["collections"]),
        "num_data_objects": len(expanded_result["data_objects"]),
        "total_data_objects_size": sum(do[1] for do in expanded_result["data_objects"]),
    }

    return expanded_result


@shared_task(name="mango_flow.irods.expand_paths", bind=True, base=MangoFlowTask)
def wrap_irods_expand_paths(
    self,
    paths: dict,
    username=None,
    delay=0,
):
    print(f"Called expand task with paths {paths}")
    time.sleep(delay)

    zone = None
    data_object_paths = paths.get("data_object_paths", [])
    collection_paths = paths.get("collection_paths", [])
    if data_object_paths:
        zone = pathlib.Path(data_object_paths[0]).parts[1]
    elif collection_paths:
        zone = pathlib.Path(collection_paths[0]).parts[1]

    if zone is None:
        return {"data_objects": [], "collections": []}

    irods_session = get_zone_operator_session(zone, username)

    return expand_paths(
        irods_session=irods_session,
        data_object_paths=data_object_paths,
        collection_paths=collection_paths,
    )


def filter_paths(
    expanded_result: dict,
    filter_lambda_data_object,
    filter_lambda_collection,
) -> dict:
    """lambda or function partial are used for yes/no filtering"""
    filtered_data_objects = filter(
        filter_lambda_data_object, expanded_result["data_objects"]
    )
    filtered_collections = filter(
        filter_lambda_collection, expanded_result["collections"]
    )
    filtered_result = {"data_objects": [], "collections": []}
    for data_object in filtered_data_objects:
        filtered_result["data_objects"].append(data_object)
    for collection in filtered_collections:
        filtered_result["collections"].append(collection)

    return filtered_result


def generate_zip_file_name(irods_session: iRODSSession):
    timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    return f"{irods_session.username}_{timestamp}.zip"


@shared_task(name="mango_flow.irods.bulk.archive_zip", bind=True, base=MangoFlowTask)
def bulk_archive_to_zip(
    self,
    expanded_paths: dict,
    username: str | None,
    zone: str,
    base_path: str,
    full_zip_archive_path: str = None,
    compression_level=9,
):
    try:
        irods_session: iRODSSession = get_zone_operator_session(zone, username)
    except Exception as e:
        print(e)
        return False
    if full_zip_archive_path is None:
        storage_dir = pathlib.Path(
            f"storage/{irods_session.zone}/tmp-zip/{irods_session.username}"
        )
        storage_dir.mkdir(parents=True, exist_ok=True)
        zip_archive_path = storage_dir / generate_zip_file_name(irods_session)
    else:
        zip_archive_path = pathlib.Path(full_zip_archive_path)
    tmp_download_dir = pathlib.Path("storage", irods_session.zone, "tmp-download")
    tmp_download_dir.mkdir(parents=True, exist_ok=True)
    # temporary file is reused, one after the other for now:
    tmp_file_timestamp = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    tmp_download_file = (
        tmp_download_dir
        / f"mf_zip_archive_tmp_file_{irods_session.username}_{tmp_file_timestamp}"
    )

    base_path = pathlib.PurePosixPath(base_path)
    buffer_size = 16 * 1024 * 104
    ## variables for intermediate status reporting
    total_do_paths = len(expanded_paths["data_objects"])
    acc_sizes = list(
        itertools.accumulate(
            [do[1] for do in expanded_paths["data_objects"]], operator.add
        )
    )
    total_size = acc_sizes[-1]  # copy last value
    # the interval is roughly 10 % steps, only accurate for large datasets
    # status_message_interval = max(1, int(total_do_paths / 10))
    count = 0
    # here we go!
    with zipfile.ZipFile(
        str(zip_archive_path),
        "w",
        zipfile.ZIP_DEFLATED,
        compresslevel=compression_level,
    ) as archive:
        for do_path, do_size, do_modified, do_checksum in expanded_paths[
            "data_objects"
        ]:
            # not yet found a way to write from a stream with a reliable library, the standard zipfile module does not do it :-(
            do = irods_session.data_objects.get(
                do_path, str(tmp_download_file), forceFlag=True
            )
            # set the modified time in the (atime, mtime) tuple (also used for the access time, which we dont have anyway)
            os.utime(str(tmp_download_file), times=(do_modified, do_modified))
            # should check the reportted size and checksum?
            self.update_state(
                state="PROGRESS",
                meta={
                    "processed": count,
                    "total": total_do_paths,
                    "current": f"{do_path}",
                    "treated_total_size": acc_sizes[count],
                    "total_size": total_size,
                    "current_size": do_size,
                },
            )
            archive.write(
                str(tmp_download_file),
                arcname=str(pathlib.PurePosixPath(do_path).relative_to(base_path)),
            )

            # if (count % status_message_interval) == 0:

            count += 1
        tmp_download_file.unlink(missing_ok=True)
    return str(zip_archive_path)
