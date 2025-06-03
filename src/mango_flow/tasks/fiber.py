from irods.session import iRODSSession
from irods.collection import iRODSCollection
from irods.meta import iRODSMeta
from celery import shared_task
from plugins.operator import get_zone_operator_session


def metadata_helper(
    irods_session: iRODSSession, collection: iRODSCollection, metadata_name: str
):
    """"""
    if metadata_name == "mgs.mango_ingest.status":
        return "completed"
    if metadata_name == "mgs.fiber_daq.project.name":
        return "<project name>"


@shared_task(name="mango_flow.tasks.fiber.ingress.construct_dest_path", bind=True)
def construct_dest_path(self, input_path: str, *args, **kwargs):
    """
    Construct the destination path for raw fiber data in the ingress folder.
    The destination path is constructed based on the input path and the metadata
    associated with the collection.
    The destination path is constructed as follows:
    /<zone>/home/FIBEr/projects/<project_name>/raw/<sample_id>/<device_name>-<acquisition_time>
    """
    # print(f"construct_dest_path: {input_path=}, {args=}, {kwargs=}")
    path_elements = input_path.split("/")
    zone = path_elements[1]
    raw_collection_name = path_elements[-1]
    # device_name_from_path = path_elements[-2]  # TRIAX ...
    sample_id = raw_collection_name.split("-")[0]
    # sample_id_timestamp = raw_collection_name.split("-")[1]

    client_user = kwargs.pop("client_user", None)

    irods_session = get_zone_operator_session(zone, client_user=client_user)

    raw_collection_irods = irods_session.collections.get(input_path)
    raw_collection_status_avu: iRODSMeta = raw_collection_irods.metadata.get_one(
        "mgs.mango_ingest.status"
    )
    device_name_avu: iRODSMeta = raw_collection_irods.metadata.get_one(
        "mgs.fiber_daq.device.name"
    )
    aquisition_time = raw_collection_irods.metadata.get_one(
        "mgs.fiber_daq.acquisition_datetime"
    )

    project_name_avu = raw_collection_irods.metadata.get_one(
        "mgs.fiber_daq.project.name"
    )

    if raw_collection_status_avu.value.strip() == "completed":
        project_path_raw = (
            "/".join(path_elements[:4]) + "/projects/" + project_name_avu.value.strip() + "/raw"
        )
        sample_path = project_path_raw + "/" + sample_id
        # sample_collection_irods = irods_session.collections.create(sample_path)
        dest_collection_name = (
            sample_path
            + "/"
            + device_name_avu.value.strip()
            + "-"
            + aquisition_time.value.replace(":", "")
        )

        # Final location to move the collection
        return (input_path, dest_collection_name)


@shared_task(name="mango_flow.tasks.fiber.ingress.validate_and_move_collection", bind=True)
def fiber_ingress_move_collection(self, path_tuple: str, *args, **kwargs):
    """
    Move the collection from the input path to the destination path with some validations

    """
    # print(f"fiber_ingress_move_collection: {path_tuple=}, {args=}, {kwargs=}")
    input_path, dest_path = path_tuple

    client_user = kwargs.pop("client_user", None)

    irods_session: iRODSSession = get_zone_operator_session(
        zone=input_path.split("/")[1], client_user=client_user
    )
    
    sub_project_path = "/".join(dest_path.split("/")[:6])  # Extract the sub-project path from the destination path

    # try to get the collection, if it does not exist, we'll get an exception and the task will fail
    try:
        irods_session.collections.get(sub_project_path)
    except Exception as e:
        raise ValueError(f"Sub project collection does not exist: {sub_project_path}") from e        

    # Move and rename the collection

    try:
        irods_session.collections.create("/".join(dest_path.split("/")[:-1]))
    except Exception as e:
        raise ValueError(f"Failed to create destination collection parent for : {dest_path}") from e
    
    irods_session.collections.move(input_path, dest_path)
    
    return dest_path