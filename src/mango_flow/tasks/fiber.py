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

    if raw_collection_status_avu.value == "completed":
        project_path_raw = (
            "/".join(path_elements[:4]) + "/projects/" + project_name_avu.value + "/raw"
        )
        sample_path = project_path_raw + "/" + sample_id
        # sample_collection_irods = irods_session.collections.create(sample_path)
        dest_collection_name = (
            sample_path
            + "/"
            + device_name_avu.value
            + "-"
            + aquisition_time.value.replace(":", "")
        )

        # Final location to move the collection
        return dest_collection_name
