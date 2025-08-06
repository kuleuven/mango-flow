# -*- coding: utf-8 -*-
import logging
import os
import pathlib
import typing

import pydantic
import yaml
from irods.session import iRODSSession
from irods.data_object import iRODSDataObject
from irods.collection import iRODSCollection
from irods.meta import AVUOperation, iRODSMeta

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
    metadata: dict, allowed: typing.Optional[list] = None, blacklist: typing.Optional[list] = None
) -> dict:
    """
    Filter metadata dictionary based on allowed and blacklist regexes.

    :param metadata: The metadata dictionary to filter.
    :param allowed: List of regex patterns to allow.
    :param blacklist: List of regex patterns to exclude. blacklist takes precedence over allowed.
    :return: Filtered metadata dictionary.

    This function should go to a more general place, as it can be used in other tasks as well.
    """
    if not allowed and not blacklist:
        return metadata

    filtered_metadata = {}

    for key, value in metadata.items():
        if allowed and not blacklist:
            if any(re.search(pattern, key) for pattern in allowed):
                filtered_metadata[key] = value
        if blacklist and not allowed:
            if not any(re.search(pattern, key) for pattern in blacklist):
                filtered_metadata[key] = value
        if allowed and blacklist:
            if any(re.search(pattern, key) for pattern in allowed) and not any(
                re.search(pattern, key) for pattern in blacklist
            ):
                filtered_metadata[key] = value

    return filtered_metadata


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
