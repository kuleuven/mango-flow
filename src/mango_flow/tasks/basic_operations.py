import datetime
import logging
import pathlib
import re
import time
import zoneinfo

from celery import shared_task
from irods.access import iRODSAccess
from irods.data_object import iRODSDataObject
from irods.exception import CollectionDoesNotExist
from irods.meta import AVUOperation
from irods.session import iRODSSession
from .. import MangoFlowTask, get_zone_operator_session, mf_apply_jinja2_template
from ..lib.patterns import mango_flow_regex_search_path, mango_flow_regex_search

logger = logging.getLogger(__name__)


class MFBasicIngestException(Exception):
    def __init__(self, message="Mango Flow basic ingest error", **params):
        super().__init__(f"{message} : {params}")


def parse_input_path_element_regex(path_element: str):
    """
    search the path element for a variable and its surroundings
    (prefix)(variable expression)(suffix)
    where variable expression is a simplified format of a regular expression between double curly braces:
    {{variable_name}}
    or
    {{variable_name:<regular expression>}}

    the allowed characters for {{variable_name}} is r"\w" or r"[a-zA-Z0-9_]", ie python variable name restrictions
    By default the <regular expression> is simply r".+"

    (prefix) and (suffix) are typically strings, but can also be regular expresions, but these are not catched in the output result
    Be sure to escape special characters . ^ $ * + ? { } [ ] \ | ( ) if they are not meant to be part of the regex
    """

    searcher = re.compile(
        r"(?P<prefix>.*){{(?P<variable_name>[\w]+)(?P<colon>:{0,1})(?P<regex>.*)}}(?P<suffix>.*)"
    )
    re_result = searcher.match(path_element)
    result = None
    if re_result:
        re_result_dict = re_result.groupdict()
        result = re_result_dict
    return result


def parse_input_path_regex(in_path: str | pathlib.PurePath) -> dict:
    # if the input is a string, convert it to a PurePath
    if isinstance(in_path, str):
        in_path = pathlib.PurePath(in_path)

    path_regex_list = []
    variable_names = []

    for path_element in in_path.parts:
        if match_result := parse_input_path_element_regex(path_element):
            variable_matcher = match_result["regex"] or ".+"
            path_regex_list.append(
                f"{match_result['prefix']}(?P<{match_result['variable_name']}>{variable_matcher}){match_result['suffix']}"
            )
            variable_names.append(match_result["variable_name"])
        else:
            path_regex_list.append(path_element)

    # omit the first (index 0) element to avoid a double //
    final_regex = "/" + "/".join(path_regex_list[1:])
    return {"regex": final_regex, "variable_names": variable_names}


def extract_variables_by_regex(
    path: str, regex: str, variable_names: list = []
) -> dict | bool:
    """
    Given a proper regex string, extracts groups from path and returns these as a dict
    or False if nothing is found
    """

    fr = re.compile(regex)
    if result := fr.search(path):
        result_dict = result.groupdict()
        if variable_names and (
            (rds := set(result_dict.keys())) == (vns := set(variable_names))
        ):
            return result_dict
        else:
            # this error should not be raised in normal situations
            raise MFBasicIngestException(missing_variables=vns.difference(rds))
    return False

def irods_ingest_move_data_object(
    irods_session: iRODSSession, src: str, dst: str, acls: dict = {}
):
    """
    src and destination are full paths
    acls is a dict with {name: access_type, ...} which will replace anything present after the move

    """

    try:
        irods_session.data_objects.move(src, dst)
        if acls:
            for user, access_type in acls.items():
                irods_session.acls.set(iRODSAccess(access_type, dst, user))

    except Exception as e:
        raise MFBasicIngestException(exception=e, src=src, dest=dst)


def irods_ingest_move_collection(
    irods_session: iRODSSession, src: str, dst: str, acls: dict = {}
):
    """"""
    try:
        irods_session.collections.move(src, dst)
        if acls:
            for user, access_type in acls.items():
                irods_session.acls.set(iRODSAccess(access_type, dst, user))

    except Exception as e:
        raise MFBasicIngestException(exception=e, src=src, dest=dst)


def make_collection_p(irods_session: iRODSSession, path, acls: dict = {}):
    """
    a mkdir -p equivalent for collections
    Take advantage that only the existence of the deepest collection
    needs to be tested, the parents will be automatically created if
    they do not exist
    """
    try:
        collection = irods_session.collections.get(path)
    except CollectionDoesNotExist:
        collection = irods_session.collections.create(path, recurse=True)
    if acls:
        for user, access_type in acls.items():
            irods_session.acls.set(iRODSAccess(access_type, path, user))
    return collection  # maybe better return just the path


def irods_ingest_copy_data_object_local(
    irods_session: iRODSSession,
    src: str,
    dst: str,
    acls: dict = {},
    metadata: list | str = "all",
):
    """ """

    try:
        irods_session.data_objects.copy(src, dst, forceFlag=True)

        if metadata:
            src_data_object = irods_session.data_objects.get(src)
            dst_data_object = irods_session.data_objects.get(dst)
            all_metadata_src = src_data_object.metadata.items()
            md_operations = []
            metadata_is_list = True if isinstance(metadata, list) else False
            for avu in all_metadata_src:
                if metadata == "all" or (metadata_is_list and avu.name in metadata):
                    md_operations.append(AVUOperation(operation="add", avu=avu))

            dst_data_object.metadata.apply_atomic_operations(*md_operations)

        if acls:
            for user, access_type in acls.items():
                irods_session.acls.set(iRODSAccess(access_type, dst, user))

    except Exception as e:
        raise MFBasicIngestException(exception=e, src=src, dest=dst)


def data_object_read_in_chunks(data_object: iRODSDataObject, read_buffer_size):
    with data_object.open("r") as f:
        position = 0
        start = time.perf_counter()
        while position < data_object.size:
            read_bytes = min(
                read_buffer_size,
                data_object.size - position,
            )
            file_chunk = f.read(read_bytes)
            bytes_read = len(file_chunk)
            position += bytes_read
            logger.info(
                f"{data_object.name}: sending {position} bytes after {time.perf_counter()-start}"
            )
            yield file_chunk


def irods_ingest_copy_data_object_streaming(
    src_irods_session: iRODSSession,
    dst_irods_session: iRODSSession,
    src: str,
    dst: str,
    acls: dict = {},
    metadata: list | str = "all",
):
    """
    Using streaming since the src and dst session may have different ACL rights
    Works also across zones
    """
    buffer_size = 2**24  # 16MiB
    src_data_object = src_irods_session.data_objects.get(src)
    dst_data_object = dst_irods_session.data_objects.create(dst)

    with dst_data_object.open("w", finalize_on_close=True) as f_dst:
        for buffer_read in data_object_read_in_chunks(src_data_object, buffer_size):
            res = f_dst.write(buffer_read)
    if metadata:
        all_metadata_src = src_data_object.metadata.items()
        md_operations = []
        metadata_is_list = True if type(metadata) == list else False
        for avu in all_metadata_src:
            if metadata == "all" or (metadata_is_list and avu.name in metadata):
                md_operations.append(AVUOperation(operation="add", avu=avu))

        dst_data_object.metadata.apply_atomic_operations(*md_operations)

    if acls:
        for user, access_type in acls.items():
            dst_irods_session.acls.set(iRODSAccess(access_type, dst, user))

    return dst


@shared_task(
    name="mango_flow.irods.ingest_move_data_object", bind=True, base=MangoFlowTask
)
def wrap_irods_ingest_move_data_object(self, path, **kwargs):
    """
    @Important! Destination template must include the object name, the collections must exist already
    @todo: split the destination templates into collection path and object naming
    """
    path_regex = kwargs.pop("path_regex")

    processed_regex = parse_input_path_regex(path_regex)["regex"]
    # @todo re-add variable checking by scanning also raw regular expressions for group names
    # possibly identifying the groups that are marked optional for flexibility
    if result := re.search(processed_regex, path):
        result_dict = result.groupdict()
        dst_template = kwargs.pop("dst_template")
        dst_path = mf_apply_jinja2_template(dst_template, **result_dict)
        client_user = kwargs.pop("client_user", None)
        zone = pathlib.Path(path).parts[1]
        irods_session = get_zone_operator_session(zone, client_user=client_user)
        irods_ingest_move_data_object(
            irods_session=irods_session,
            src=path,
            dst=dst_path,
            acls=kwargs.get("acls", {}),
        )

        return dst_path

    else:
        # raise an error in case of no match to break any further processing in chains
        raise (MFBasicIngestException(no_match_move_do=path))


def check_file_age(dobj):
    utc = zoneinfo.ZoneInfo("UTC")
    now = datetime.datetime.now(tz=utc)
    difference = now - dobj.modify_time.replace(tzinfo=utc)
    return difference.total_seconds()


@shared_task(name="mango_flow.irods.test_offset", bind=True, base=MangoFlowTask)
def wrap_test_offset(self, path, client_user):
    zone = pathlib.Path(path).parts[1]
    irods_session = get_zone_operator_session(zone, client_user=client_user)
    dobj = irods_session.data_objects.get(path)
    total_seconds = check_file_age(dobj)
    if total_seconds < 60:
        return f"{path} is only {int(total_seconds)} seconds old, I will skip it"
    return f"{path} is already {int(total_seconds)} seconds old!"


@shared_task(
    name="mango_flow.irods.test_pattern_and_jinja_transform",
    bind=True,
    base=MangoFlowTask,
)
def test_pattern_and_jinja_transform(self, path, path_regex, dst_template, **kwargs):
    """
    A task to test the path regex and jinja2 template transformation on a given irods path"
    """
    result_dict = mango_flow_regex_search_path(
        mango_regex_pattern=path_regex, path=path, 
    )

    # decorate the result with the irods object if any:
    client_user = kwargs.pop("client_user", None)
    irods_session = get_zone_operator_session(
        pathlib.Path(path).parts[1], client_user=client_user
    )
    for key, value in result_dict.items():
        try:
            irods_item = irods_session.data_objects.get(value["path"])
            result_dict[key]["item"] = irods_item
            result_dict[key]["item_type"] = "data_object"

        except Exception as e:
            try:
                irods_item = irods_session.collections.get(value["path"])
                result_dict[key]["item"] = irods_item
                result_dict[key]["item_type"] = "collection"
            except Exception as e:
                result_dict[key]["item"] = None

    if result_dict:
        logger.info(f"Path {path} matches regex {path_regex} with result {result_dict}")

        jinja2_result = mf_apply_jinja2_template(dst_template, **result_dict)
        logger.info(
            f"Destination path for {path} is {jinja2_result} using template {dst_template}"
        )
        return jinja2_result
    else:
        logger.error(
            f"Path {path} does not match regex {path_regex}, no transformation applied"
        )
        # raise MFBasicIngestException(no_match_pattern=path, pattern=path_regex)
        return path


@shared_task(
    name="mango_flow.irods.ingest_copy_data_object", bind=True, base=MangoFlowTask
)
def wrap_irods_ingest_copy_data_object(self, path, **kwargs):
    """"""

    path_regex = kwargs.pop("path_regex")

    processed_regex = parse_input_path_regex(path_regex)["regex"]
    # @todo re-add variable checking by scanning also raw regular expressions for group names
    if result := re.search(processed_regex, path):
        result_dict = result.groupdict()
        dst_template = kwargs.pop("dst_template")
        dst_path = mf_apply_jinja2_template(dst_template, **result_dict)
        client_user = kwargs.pop("client_user", None)
        dst_client_user = kwargs.pop("dst_client_user", None)

        zone = pathlib.Path(path).parts[1]
        dst_zone = pathlib.Path(dst_path).parts[1]

        if dst_client_user or zone != dst_zone:
            src_irods_session = get_zone_operator_session(zone, client_user=client_user)
            dst_irods_session = get_zone_operator_session(
                dst_zone, client_user=(dst_client_user or client_user)
            )
            irods_ingest_copy_data_object_streaming(
                src_irods_session=src_irods_session,
                dst_irods_session=dst_irods_session,
                src=path,
                dst=dst_path,
                acls=kwargs.get("acls", {}),
                metadata=kwargs.get("metadata", "all"),
            )

        else:
            irods_session = get_zone_operator_session(zone, client_user=client_user)

            irods_ingest_copy_data_object_local(
                irods_session=irods_session,
                src=path,
                dst=dst_path,
                acls=kwargs.get("acls", {}),
                metadata=kwargs.get("metadata", "all"),
            )

        return dst_path

    else:
        # raise an error in case of no match to break any further processing in chains
        raise (MFBasicIngestException(no_match_copy_do=path))


@shared_task(
    name="mango_flow.irods.ingest_move_collection", bind=True, base=MangoFlowTask
)
def wrap_irods_move_collection(self, path, **kwargs):
    """"""
    """
    @Important! Destination template must include the object name, the collections must exist already
    @todo: split the destination templates into collection path and object naming
    """
    path_regex = kwargs.pop("path_regex")

    processed_regex = parse_input_path_regex(path_regex)["regex"]
    # @todo re-add variable checking by scanning also raw regular expressions for group names
    # possibly identifying the groups that are marked optional for flexibility
    if result := re.search(processed_regex, path):
        result_dict = result.groupdict()
        dst_template = kwargs.pop("dst_template")
        dst_path = mf_apply_jinja2_template(dst_template, **result_dict)
        client_user = kwargs.pop("client_user", None)
        zone = pathlib.Path(path).parts[1]
        irods_session = get_zone_operator_session(zone, client_user=client_user)
        irods_ingest_move_collection(
            irods_session=irods_session,
            src=path,
            dst=dst_path,
            acls=kwargs.get("acls", {}),
        )

        return dst_path

    else:
        # raise an error in case of no match to break any further processing in chains
        raise (MFBasicIngestException(no_match_move_do=path))
