# -*- coding: utf-8 -*-
import logging
import os
import pathlib
import re
import time
import typing

import jsonlines
import pydantic
import yaml
from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.meta import AVUOperation, iRODSMeta
from irods.session import iRODSSession

from .. import get_zone_operator_session

logger = logging.getLogger(__name__)


def read_yaml_from_string(input_string: str) -> dict:
    """
    Read a YAML string and return a dictionary.
    """
    try:
        return yaml.safe_load(input_string)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML format: {e}")
    except Exception as e:
        raise ValueError(f"Error reading YAML: {e}")


def read_yaml_from_data_object(path: str, irods_session: iRODSSession) -> dict:
    """
    Read a YAML file directly from iRODS and return a dictionary.
    """
    try:
        data_object = irods_session.data_objects.get(path)
        with data_object.open("r") as file:
            yaml_string = file.read()
        return read_yaml_from_string(yaml_string)
    except Exception as e:
        raise ValueError(f"Error reading YAML from data object: {e}")


def write_yaml_to_data_object(
    path: str, yaml_contents: dict, irods_session: iRODSSession
) -> None | iRODSDataObject:
    """
    Write a YAML file directly to iRODS
    """
    try:
        data_object = irods_session.data_objects.create(path)
        with data_object.open("w") as file:
            yaml_string = yaml.dump(yaml_contents)
            file.write(yaml_string.encode())
        return data_object
    except Exception as e:
        raise ValueError(f"Error writing YAML to data object: {e}")


def get_list_diff(old: list, new: list) -> dict:
    """
    Compare two lists and return the added and removed items.
    """
    added = [item for item in new if item not in old]
    removed = [item for item in old if item not in new]
    return {"added": added, "removed": removed}


def load_yaml_config(
    from_env_var: str = "",
    from_path: str = "",
    pd_model: pydantic.BaseModel | None = None,
    raise_error: bool = True,
) -> dict | None:
    """
    Load load a YAML configuration from an environment variable or a file path
    and validate it against a Pydantic model if provided.
    The environment variable has precedence over the file path.
    If raise_error is True, it will raise an error if the YAML cannot be loaded or validated,
    otherwise it will log a warning and return None.
    """
    yaml_config = {}

    if yaml_path := (os.getenv(from_env_var, "") or from_path):
        try:
            yaml_config = yaml.safe_load(pathlib.Path(yaml_path).read_text())
        except Exception as e:
            if raise_error:
                raise RuntimeError(f"Failed to load configuration from file: {e}")
            else:
                logger.warning(f"Warning: Failed to load configuration from file: {e}")

    if yaml_config and pd_model:
        try:
            pd_model.model_validate(yaml_config)
            return yaml_config
        except pydantic.ValidationError as e:
            if raise_error:
                raise RuntimeError(f"Failed to validate configuration: {e}")
            else:
                logger.warning(f"Warning: Failed to validate configuration: {e}")

    # if we are here
    if raise_error:
        raise RuntimeError(
            f"Failed to validate configuration {from_env_var=}, {from_path=}"
        )

    return None


def get_irods_item(
    irods_session: iRODSSession, path: str
) -> typing.Union[iRODSDataObject, iRODSCollection, None]:
    """
    Get an iRODS data object or collection by its path.
    Returns None if the item does not exist.
    """
    try:
        return irods_session.data_objects.get(path)
    except Exception:
        try:
            return irods_session.collections.get(path)
        except Exception:
            return None


def filter_metadata_dict(
    metadata: dict,
    allowed: typing.Optional[list] = [],
    blacklist: typing.Optional[list] = [],
) -> dict:
    """
    Filter metadata dictionary based on allowed and blacklist regexes.

    :param metadata: The metadata dictionary to filter.
    :param allowed: List of regex patterns to allow.
    :param blacklist: List of regex patterns to exclude. blacklist takes precedence over allowed.
    :return: Filtered metadata dictionary.
    """

    # if there are no criteria, treat it as allowed
    if not allowed and not blacklist:
        return metadata

    filtered_metadata = {}

    def is_allowed(key):
        return any(re.search(pattern, key) for pattern in allowed)

    def not_blacklisted(key):
        return not any(re.search(pattern, key) for pattern in blacklist)

    filtered_metadata = (
        {key: value for key, value in filtered_metadata.items() if is_allowed(key)}
        if allowed
        else metadata
    )
    return (
        {key: value for key, value in filtered_metadata.items() if not_blacklisted(key)}
        if blacklist
        else filtered_metadata
    )


def bulk_add_metadata(
    item: iRODSDataObject | iRODSCollection,
    metadata_items: dict,
    unit_text: str = "analysis/other",
    wipe_matching_unit=True,
    as_admin=False,
):
    if metadata_items:
        metadata_names = metadata_items.keys()
        avu_operations = [
            AVUOperation(operation="remove", avu=avu)
            for avu in item.metadata.items()
            # match any unit that starts with the string specified.
            # The general wisdom is that is used in a mime like or namespace fashion seprateed with forward slashes
            # eg 'analysis/'
            if (avu.name in metadata_names)
            or (wipe_matching_unit and avu.units.startswith(unit_text))
        ]
        for m_name, m_value in metadata_items.items():
            if m_value is list:
                avu_operations.extend(
                    [
                        AVUOperation(
                            "add",
                            iRODSMeta(name=m_name, value=sub_value, units=unit_text),
                        )
                        for sub_value in m_value
                    ]
                )

            elif m_value is str:
                avu_operations.append(
                    AVUOperation(
                        operation="add",
                        avu=iRODSMeta(name=m_name, value=m_value, units=unit_text),
                    )
                )

        if num_operations := len(avu_operations):
            item.metadata(admin=as_admin).apply_atomic_operations(*avu_operations)
            return num_operations


def get_zone_operator_session_zone_via_path(
    path: str, client_user: str | None = None
) -> iRODSSession | str:
    zone = pathlib.Path(path).parts[1]
    try:
        return get_zone_operator_session(zone, client_user=client_user)
    except Exception as e:
        return str(e)


def fetch_all_irods_item_paths_for_subtree(
    irods_session: iRODSSession,
    subtree_path: str,
    item_type: str = "data_object",
    sort: bool = False,
    local_file_name: str | None = None,
    local_file_type: str = "jsonl",  # only jsonl for now, used as extension if none is specified in filename
    batch_size: int = 100_000,  # only useful in case huge amount of items and results should be recorded in a file
    iteration_call_back: (
        typing.Callable | None
    ) = None,  # will mostly be progress reporting if set
):
    COUNT_COLUMN = "DATA_ID" if item_type == "data_object" else "COLL_ID"
    SELECT_COLUMS = (
        "DISTINCT COLL_NAME, DATA_NAME, DATA_SIZE, DATA_MODIFY_TIME, DATA_CHECKSUM"
        if item_type == "data_object"
        else "DISTINCT COLL_NAME, COLL_MODIFY_TIME"
    )
    SORT_EXPRESSION = ""  # no sorting for now, performance impact and consistency risk in batch processing

    q_result = irods_session.genquery2(
        f"SELECT COUNT(DISTINCT {COUNT_COLUMN}) where COLL_NAME = '{subtree_path}' OR COLL_NAME like '{subtree_path}/%' {SORT_EXPRESSION}"
    )

    total = int(q_result[0][0])

    all_items = []
    total_size = 0
    total_processed = 0
    start_time = time.time()
    iteration = 0
    if local_file_name:
        # check if the proper file name extension is present and add it if not
        local_file_name = (
            local_file_name
            if local_file_name.endswith(local_file_type)
            else ".".join([local_file_name, local_file_type])
        )
        # deal with different file types @future
        if local_file_type == "jsonl":
            item_file = jsonlines.open(local_file_name, "w")
        else:
            raise (ValueError)
    try:
        # start batch processing
        for offset in range(0, total, batch_size):
            start_time_iteration = time.time()
            iteration += 1
            iteration_items = []
            batch_total_size = 0

            q_result = irods_session.genquery2(
                f"SELECT {SELECT_COLUMS} "
                f"WHERE COLL_NAME = '{subtree_path}' OR COLL_NAME like '{subtree_path}/%' LIMIT {batch_size} OFFSET {offset}"
            )
            for item in q_result:
                if item_type == "data_object":
                    item_dict = {
                        "path": f"{item[0]}/{item[1]}",
                        "size": int(item[2]),
                        "modified": int(f"0{item[3]}"),
                        "checksum": item[4],
                    }
                    batch_total_size += int(item[2])
                else:
                    item_dict = {
                        "path": item[0],
                        "modified": int(f"0{item[1]}"),
                    }
                iteration_items.append(item_dict)

            total_processed += len(q_result)
            total_size += batch_total_size

            if local_file_name:
                if local_file_type == "jsonl":
                    item_file.write_all(iteration_items)
                else:
                    raise (ValueError)
            else:
                all_items += iteration_items
            if iteration_call_back:
                current_time = time.time()
                iteration_call_back(
                    {
                        "item_type": item_type,
                        "stats": {
                            "iteration": iteration,
                            "batch_processed": len(q_result),
                            "batch_total_bytes": batch_total_size,
                            "total_processed": total_processed,
                            "total_bytes": total_size,
                            "iteration_time": start_time_iteration - current_time,
                            "total_time": start_time - current_time,
                        },
                        "status": "ITERATION_SUCCESS",
                    }
                )
    except Exception as e:
        raise (e)
    finally:
        if local_file_name:
            item_file.close()

    return {
        "type": "file_jsonl" if local_file_name else "list",
        "stats": {"total_size": total_size, "total_items": total},
        "payload": local_file_name if local_file_name else all_items,
    }
