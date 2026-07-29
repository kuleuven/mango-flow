import logging
import pathlib
import re
import tempfile

import multidict
from celery import shared_task

from .. import MangoFlowTask, get_zone_operator_session
from ..lib.patterns import mango_flow_regex_search, translate_mango_moustache_regex
from ..lib.util import bulk_add_metadata

logger = logging.getLogger(__name__)


class MFMetaDataExtractionError(Exception):
    def __init__(self, message="Mango Flow Metadata Extraction Error", **params):
        super().__init__(f"{message} : {params}")


def extract_metadata_from_path(
    path: str,
    path_regex: str,
    mapper: dict = {},
    split_metadata: dict = {},
    value_mapper: dict = {},
    apply_first_value_mapping_only: bool = True,
) -> dict:
    """
    both path and path_regex path may be a partial path expression, meaning from the end of a string
    mapper converts the restricted metadata names into a more general form
    split_metadata is used to further split a value into a list of values
    and contains the metadata name (before mapping) and the regex to split on
    value_mapper is a dictionary of dictionaries, where the first key is the metadata name (before mapping)
    and the second key is the value to be mapped via a regex. This is meant to normalize values
    so the secondary keys are regular expressions that can map the value to a new/common value.
    Multiple regular expressions can be used to map the source value to different destination values

    Note that the value_mapper and split_metadata are not to be used at the same time for now

    """
    extracted_metadata_raw = mango_flow_regex_search(path_regex, path)

    extracted_metadata = {}
    if extracted_metadata_raw:

        for key, value in extracted_metadata_raw.items():
            if key in value_mapper:
                for value_regex in value_mapper[key]:
                    if re.search(value_regex, value):
                        value = re.sub(
                            value_regex, value_mapper[key][value_regex], value
                        )
                        if apply_first_value_mapping_only:
                            break  # the first match is used
            if key in split_metadata:
                # logger.info(f"Splitting {key} with {split_metadata[key]}")
                value = re.split(split_metadata[key], value)

            extracted_metadata[mapper.get(key, key)] = value

    return extracted_metadata


def extract_metadata_from_marked_file_header(
    filename: str,
    header_line_start_re: str = r"[%#\*]+",
    name_value_split_re: str = r"\s*[:=]\s*",
    **strategies,
) -> dict:
    """ """
    # the metadata multidict is supposed to be 1 level deep, catching possible multivalued metadata from (key, value) tuples
    metadata = multidict.MultiDict()

    with open(filename, "r") as f:
        num_non_headerlines = 0
        num_lines = 0
        max_num_non_header_lines = strategies.get("max_num_non_header_lines", 5)
        before_name_re = strategies.get("before_name_re", r"\s*")
        after_value_re = strategies.get("after_value_re", r"\s*")
        while True:
            current_line = f.readline()
            num_lines += 1
            # Check if we have a marker for the current line being part of the header, usually some characters to denote a comment
            if re.match(header_line_start_re, current_line):
                # if so, try to extract netadata name and value pairs
                metadata_match: re.Match = re.match(
                    f"{header_line_start_re}{before_name_re}(?P<variable_name>.*)(?P<separator>{name_value_split_re})(?P<variable_value>.*){after_value_re}",
                    current_line,
                )
                if metadata_match:
                    result_dict = metadata_match.groupdict()
                    # logger.info(f"Captured metadata: {result_dict}")
                    metadata.add(
                        result_dict["variable_name"], result_dict["variable_value"]
                    )
            else:
                num_non_headerlines += 1
            if num_non_headerlines > max_num_non_header_lines:
                break

    # Convert to a one-level nested dict, with multivalued metadata as list, single valued as scalar
    metadata_as_dict = {}
    for key in metadata.keys():
        value = metadata.getall(key)
        metadata_as_dict[key] = value[0] if len(value) == 1 else value
    return metadata_as_dict


@shared_task(
    name="mango_flow.irods.extract_metadata_path", bind=True, base=MangoFlowTask
)
def wrap_extract_metadata_from_path(self, path, **kwargs):
    path_regex = kwargs["path_regex"]

    if not re.search(path_regex, path):
        return path

    zone = pathlib.Path(path).parts[1]

    logger.info(f"Entering metadata extraction for zone {zone}, path {path}")
    client_user = kwargs.pop("client_user", None)
    irods_session = get_zone_operator_session(zone, client_user=client_user)
    # @todo: make parameters explicit
    metadata_items = extract_metadata_from_path(path, **kwargs)
    logger.info(f"Here is what I found: {metadata_items.keys()}")
    data_object = irods_session.data_objects.get(path)
    as_admin = False if client_user else True
    bulk_add_metadata(
        data_object, metadata_items, unit_text="analysis/path", as_admin=as_admin
    )

    return path


@shared_task(
    name="mango_flow.irods.extract_metadata_path_multi_regex",
    bind=True,
    base=MangoFlowTask,
)
def wrap_extract_metadata_from_path_multi_regex(
    self,
    path,
    path_regex_list,
    mapper,
    split_metadata,
    apply_first_match_only=True,
    **kwargs,
):

    matching_regexes = []

    for path_regex in path_regex_list:
        # allow both moustache like syntax as well as full regex strings
        path_regex = translate_mango_moustache_regex(path_regex)
        if re.search(path_regex, path):
            matching_regexes.append(path_regex)
            if apply_first_match_only:
                break

    if not matching_regexes:
        return path

    zone = pathlib.Path(path).parts[1]

    client_user = kwargs.pop("client_user", None)
    irods_session = get_zone_operator_session(zone, client_user=client_user)
    metadata_items = {}
    for path_regex in matching_regexes:
        metadata_items |= extract_metadata_from_path(
            path, path_regex=path_regex, mapper=mapper, split_metadata=split_metadata
        )
    data_object = irods_session.data_objects.get(path)
    as_admin = False if client_user else True
    bulk_add_metadata(
        data_object, metadata_items, unit_text="analysis/path", as_admin=as_admin
    )

    return path


@shared_task(
    name="mango_flow.irods.extract_metadata_marked_text_file_header",
    bind=True,
    base=MangoFlowTask,
)
def wrap_extract_metadata_from_marked_file_header(self, path, **kwargs):
    path_regex = kwargs.pop("path_regex")
    # allow both moustache as well as full python regex strings
    if not re.search(translate_mango_moustache_regex(path_regex), path):
        return path

    client_user = kwargs.pop("client_user", None)
    buffer_size = kwargs.pop("buffer_size", 50 * 1024)
    zone = pathlib.Path(path).parts[1]
    irods_session = get_zone_operator_session(zone, client_user=client_user)

    data_object = irods_session.data_objects.get(path)
    with data_object.open("r") as f:
        blub = f.read(buffer_size)
    if blub:
        logger.info("Creating temp file for header extraction")
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w+", encoding="utf-8"
        ) as fp:
            fp.write(blub.decode("utf-8"))
            fp.close()
            # @todo add a filter
            metadata_items = extract_metadata_from_marked_file_header(
                filename=fp.name, **kwargs
            )
            # logger.info(f"Extracted metadata from header: {metadata_items}")
            as_admin = False if client_user else True
            bulk_add_metadata(
                data_object,
                metadata_items,
                unit_text="analysis/text-header",
                as_admin=as_admin,
            )

    return path
