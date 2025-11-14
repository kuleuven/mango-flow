from irods.data_object import iRODSDataObject
from irods.collection import iRODSCollection


def get_metadata_as_dict_for_sidecar(item: iRODSDataObject | iRODSCollection) -> dict:
    return {}


def get_bagit_contents() -> str:
    return '  '


def get_ro_crate(manifest_fp, user_metadata: dict):
    pass
