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
                raise RuntimeError(
                    f"Failed to load configuration from file: {e}"
                )
            else:
                logger.warning(
                    f"Warning: Failed to load configuration from file: {e}"
                )

    if yaml_config and pd_model:
        try:
            pd_model.model_validate(yaml_config)
            return yaml_config
        except pydantic.ValidationError as e:
            if raise_error:
                raise RuntimeError(
                    f"Failed to validate configuration: {e}"
                )
            else:
                logger.warning(
                    f"Warning: Failed to validate  configuration: {e}"
                )

    # if we are here
    if raise_error:
        raise RuntimeError(f"Failed to validate configuration {from_env_var=}, {from_path=}")

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
