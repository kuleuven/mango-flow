import json
import logging
import pathlib
import re
import tempfile
from typing import Optional

from celery import shared_task
from exiftool import ExifToolHelper
from irods.session import iRODSSession

from .. import get_zone_operator_session
from ..lib.util import bulk_add_metadata, filter_metadata_dict

logger = logging.getLogger(__name__)

# start the exiftool helper, it uses a daemon mode
md_extractor = ExifToolHelper()


def extract(path: str):
    """
    Extract metadata from a file using ExifTool.
    :param path: The path to the file from which to extract metadata.
    """

    # too crude? think (t)wi(c|s)e ;-)
    logger.info(f"exiftool extraction requested from {path} ")

    try:
        exiftool_metadata = md_extractor.get_metadata(path)
        return exiftool_metadata
    except Exception as e:
        logger.warning(f"Exception in metadata extraction: {e}")
        return {}


@shared_task(name="mango_flow.tasks.exiftool_metadata_extraction.extract", bind=True)
def exiftool_extract_metadata_task(self, path: str, **kwargs):
    """
    Extract metadata from a file using ExifTool.
    :param path: The path to the file from which to extract metadata.
    """

    if allowed := kwargs.pop("allowed", None):
        matching_allowed = False
        for allow_regex in allowed:
            if not isinstance(allow_regex, str):
                logger.error(f"Allowed regex {allow_regex} is not a string, skipping.")
                continue
            if re.search(allow_regex, path):
                logger.info(f"Path {path} matches allowed regex {allow_regex}.")
                matching_allowed = True
                # if kwargs.get("apply_first_match_only", True):
                #     break

        if not matching_allowed:
            logger.info(f"Path {path} does not match any allowed regexes, skipping.")
            return path

    if blacklist := kwargs.pop("blacklist", [r"\.json$"]):
        for blacklist_regex in blacklist:
            if not isinstance(blacklist_regex, str):
                logger.error(
                    f"Blacklist regex {blacklist_regex} is not a string, skipping."
                )
                continue
            if re.search(blacklist_regex, path):
                logger.info(
                    f"Path {path} matches blacklist regex {blacklist_regex}, skipping."
                )
                return path

    mode = kwargs.pop("mode", "sidecar")

    client_user = kwargs.pop("client_user", None)
    zone = pathlib.Path(path).parts[1]
    irods_session: iRODSSession = get_zone_operator_session(
        zone, client_user=client_user
    )

    # max_size = kwargs.pop("max_size", 500)

    metadata = None

    try:
        data_object = irods_session.data_objects.get(path)
        # TODO: Add root path, as files can become too big in for example a container
        with tempfile.TemporaryDirectory() as tmpdirname:
            local_path = pathlib.Path(tmpdirname) / data_object.name
            irods_session.data_objects.get(path, str(local_path))
            metadata = extract(local_path)[
                0
            ]  # extract returns in principle a list of metadata dictionaries, we take the first one as we are operating on a single file
            logger.info(f"Metadata extracted from {local_path}: {metadata}")
    except Exception as e:
        logger.error(f"Failed to get data object for path {path}: {e}")
        return {}

    if not metadata:
        logger.warning(f"No metadata extracted from {path}.")
        return path

    # Filter metadata if allowed_metadata or blacklist_metadata regexes are provided
    allowed_metadata = kwargs.pop("allowed_metadata", None)
    blacklist_metadata = kwargs.pop("blacklist_metadata", None)
    if allowed_metadata or blacklist_metadata:
        metadata = filter_metadata_dict(
            metadata, allowed=allowed_metadata, blacklist=blacklist_metadata
        )
        logger.info(f"Filtered metadata: {metadata}")

    if mode == "sidecar":
        sidecar_ext = kwargs.pop("sidecar_ext", "metadata.json")
        if path.endswith(sidecar_ext):
            logger.warning(
                f"Path {path} already ends with sidecar extension {sidecar_ext}, skipping."
            )
            return {}
        sidecar_path = f"{path}.{sidecar_ext}"
        try:
            with tempfile.NamedTemporaryFile(mode="w+t") as sidecar_tempfile:
                # Write metadata to the sidecar file via a tempfile as json.dump expects to write text, not binary data
                json.dump(metadata, sidecar_tempfile, indent=2)
                sidecar_tempfile.flush()
                # Upload the sidecar file to iRODS
                irods_session.data_objects.put(
                    str(sidecar_tempfile.name), sidecar_path, force=True
                )

            logger.info(f"Metadata written to sidecar fildata object: {sidecar_path}")
        except Exception as e:
            logger.error(
                f"Failed to write metadata to sidecar file {sidecar_path}: {e}"
            )
            return {}

    elif mode == "metadata":
        try:
            for metadata_dict in metadata:
                bulk_add_metadata(
                    data_object,
                    metadata_dict,
                    unit_text="exiftool",
                    as_admin=not client_user,
                )
                logger.info(f"Metadata added to iRODS data object: {path}")
        except Exception as e:
            logger.error(f"Failed to add metadata to iRODS data object {path}: {e}")
            return {}
    elif mode == "propagate":
        # propagate metadata to the next task, returning the path along the metadata
        return {
            "path": path,
            "metadata": metadata,
        }
    else:
        logger.error(
            f"Unknown mode {mode} for metadata extraction. metadata is ignored."
        )
        # Maybe we should raise an exception here?

    return path
