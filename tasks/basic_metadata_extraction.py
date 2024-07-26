import re
import multidict
import pathlib
import tempfile

from celery import shared_task
from plugins.operator import get_zone_operator_session
from irods.meta import AVUOperation, iRODSMeta
from irods.session import iRODSSession
from irods.data_object import iRODSDataObject
from irods.collection import iRODSCollection
import pprint

from plugins.mango_flow import MangoFlowTask

class MFMetaDataExtractionError(Exception):
    def __init__(self, message="Mango Flow Metadata Extraction Error", **params):
        super().__init__(f"{message} : {params}")


def extract_metadata_from_path(
    path: str, path_regex: str, mapper: dict = {}, split_metadata: dict = {}
) -> dict:
    """
    both path and path_regex path may be a partial path expression, meaning from the end of a string
    mapper converts the restricted metadata names into a more general form (irods accepts almost anything)
    split_metadata is used to further split a value into a list of values
    and contains the metadata name (before mapping) and the regex to split on
    """
    matches = re.search(path_regex, path)

    extracted_metadata = {}
    if matches:
        extracted_metadata_raw = matches.groupdict()

        for key, value in extracted_metadata_raw.items():
            extracted_metadata[mapper.get(key, key)] = (
                value
                if not split_metadata.get(key, False)
                else re.split(split_metadata[key], value)
            )

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
                    # print(f"Captured metadata: {result_dict}")
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


def bulk_add_metadata(item : iRODSDataObject| iRODSCollection, metadata_items: dict, unit_text: str = "analysis/other", as_admin = False):
    if metadata_items:
        metadata_names = metadata_items.keys()
        avu_operations = [
            AVUOperation(operation="remove", avu=avu)
            for avu in item.metadata.items()
            if avu.name in metadata_names
        ]
        for m_name, m_value in metadata_items.items():
            if type(m_value) == list:
                avu_operations.extend(
                    [
                        AVUOperation(
                            "add",
                            iRODSMeta(
                                name=m_name, value=sub_value, units=unit_text
                            ),
                        )
                        for sub_value in m_value
                    ]
                )

            elif type(m_value) == str:
                avu_operations.append(
                    AVUOperation(
                        operation="add",
                        avu=iRODSMeta(
                            name=m_name, value=m_value, units=unit_text
                        ),
                    )
                )
            else:
                raise (MFMetaDataExtractionError({"unknown_field_type": type(m_value)}))
        if len(avu_operations):
            #pprint.pp(avu_operations)
            item.metadata(admin=as_admin).apply_atomic_operations(*avu_operations)



@shared_task(name="mango_flow.irods.extract_metadata_path", bind=True, base=MangoFlowTask)
def wrap_extract_metadata_from_path(self, path, **kwargs):
    path_regex = kwargs["path_regex"]

    if not re.search(path_regex, path):
        return path

    zone = pathlib.Path(path).parts[1]

    print(f"Entering metadata extraction for zone {zone}, path {path}")
    client_user = kwargs.pop('client_user', None)
    irods_session = get_zone_operator_session(zone, client_user=client_user)
    # @todo: make parameters explicit
    metadata_items = extract_metadata_from_path(**kwargs)
    pprint.pp(f"Here is what I found: {metadata_items.keys()}")
    data_object = irods_session.data_objects.get(path)
    as_admin = False if client_user else True
    bulk_add_metadata(data_object, metadata_items, unit_text="analysis/path", as_admin=as_admin)
    
    return path


@shared_task(name="mango_flow.irods.extract_metadata_marked_text_file_header", bind=True, base=MangoFlowTask)
def wrap_extract_metadata_from_marked_file_header(self, path, **kwargs):
    path_regex = kwargs.pop('path_regex')
    if not re.search(path_regex, path):
        return path

    client_user = kwargs.pop('client_user', None)
    buffer_size = kwargs.pop('buffer_size', 50*1024)
    zone = pathlib.Path(path).parts[1]
    irods_session = get_zone_operator_session(zone, client_user=client_user)
    
    data_object = irods_session.data_objects.get(path)
    with data_object.open("r") as f:
        blub = f.read(buffer_size)
    if blub:
        print(f"Creating temp file for header extraction")
        with tempfile.NamedTemporaryFile(delete=False, mode = "w+", encoding='utf-8') as fp:
            fp.write(blub.decode('utf-8'))
            fp.close()
            #@todo add a filter 
            metadata_items = extract_metadata_from_marked_file_header(filename = fp.name, **kwargs )
            #print(f"Extracted metadata from header: {metadata_items}")
            as_admin = False if client_user else True
            bulk_add_metadata(data_object, metadata_items, unit_text="analysis/text-header", as_admin=as_admin)

    return path
