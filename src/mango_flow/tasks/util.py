# -*- coding: utf-8 -*-
import yaml
from irods.session import iRODSSession


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
) -> dict:
    """
    Read a YAML file directly from iRODS and return a dictionary.
    """
    data_object = irods_session.data_objects.create(path)
    with data_object.open("w") as file:
        yaml_string = yaml.dump(yaml_contents)
        file.write(yaml_string.encode())


def get_list_diff(old: list, new: list) -> dict:
    """
    Compare two lists and return the added and removed items.
    """
    added = [item for item in new if item not in old]
    removed = [item for item in old if item not in new]
    return {"added": added, "removed": removed}
