from irods.session import iRODSSession
from irods.collection import iRODSCollection
from irods.meta import iRODSMeta
from irods.access import iRODSAccess
from celery import shared_task
from plugins.operator import get_zone_operator_session

import logging


def metadata_helper(
    irods_session: iRODSSession, collection: iRODSCollection, metadata_name: str
):
    """"""
    if metadata_name == "mgs.mango_ingest.status":
        return "completed"
    if metadata_name == "mgs.fiber_daq.project.name":
        return "<project name>"


@shared_task(name="mango_flow.tasks.fiber.ingress.construct_dest_path", bind=True)
def construct_dest_path(self, input_path: str, **kwargs):
    """
    Construct the destination path for raw fiber data in the ingress folder.
    The destination path is constructed based on the input path and the metadata
    associated with the collection.
    The destination path is constructed as follows:
    /<zone>/home/FIBEr/projects/<project_name>/raw/<sample_id>/<device_name>-<acquisition_time>

    @todo: refactor this in a more general task that can be reused for other uses
    
    """
    print(f"construct_dest_path: {input_path=}, {kwargs=}")
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
            "/".join(path_elements[:4])
            + "/projects/"
            + project_name_avu.value.strip()
            + "/raw"
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


@shared_task(
    name="mango_flow.tasks.fiber.ingress.validate_and_move_collection", bind=True
)
def fiber_ingress_move_collection(self, path_tuple, **kwargs):
    """
    Move the collection from the input path to the destination path with some validations

    """
    print(f"fiber_ingress_move_collection: {path_tuple=}, {kwargs=}")
    input_path, dest_path = path_tuple

    client_user = kwargs.pop("client_user", None)
    project_must_exist = kwargs.pop("project_must_exist", True)

    irods_session: iRODSSession = get_zone_operator_session(
        zone=input_path.split("/")[1], client_user=client_user
    )

    sub_project_path = "/".join(
        dest_path.split("/")[:6]
    )  # Extract the sub-project path from the destination path

    # try to get the collection, if it does not exist, we'll get an exception and the task will fail
    if project_must_exist:
        try:
            irods_session.collections.get(sub_project_path)
        except Exception as e:
            raise ValueError(
                f"Sub project collection does not exist: {sub_project_path}"
            ) from e

    # Move and rename the collection

    try:
        irods_session.collections.create("/".join(dest_path.split("/")[:-1]))
    except Exception as e:
        raise ValueError(
            f"Failed to create destination collection parent for : {dest_path}"
        ) from e

    irods_session.collections.move(input_path, dest_path)

    return dest_path


@shared_task(name="mango_flow.tasks.fiber.ingress.enfore_acls_parent", bind=True)
def fiber_ingress_enforce_acls_parent(self, dest_path: str, **kwargs):
    """
    Enforce ACLs on the parent collection of the destination path. This is especially useful for move operations 
    where the parent collections typically has or get the ACLs through inheritance

    @todo move and rename to users module
    """
    print(f"fiber_ingress_enforce_acls_parent: {dest_path=}, {kwargs=}")
    # dest_path = dest_path[-1] # this is weird, but the path_tuple is a tuple with one element, so we need to extract it
    # print(f"fiber_ingress_enforce_acls_parent: new {dest_path=} with type={type(dest_path)}")
    client_user = kwargs.pop("client_user", None)
    print(f"fiber_ingress_enforce_acls_parent: {client_user=}")
    recursive = kwargs.pop("recursive", True)
    print(f"fiber_ingress_enforce_acls_parent: {recursive=}")
    discard_existing_acls = kwargs.pop("discard_existing_acls", False)

    irods_session: iRODSSession = get_zone_operator_session(
        zone=dest_path.split("/")[1], client_user=client_user
    )

    collection: iRODSCollection = irods_session.collections.get(dest_path)

    parent_collection = irods_session.collections.get(
        "/".join(dest_path.split("/")[:-1])
    )

    parent_acls = irods_session.acls.get(parent_collection, report_raw_acls=True)
    current_acls = irods_session.acls.get(
        collection, report_raw_acls=True,
    )

    if discard_existing_acls:
        # Discard existing ACLs on the collection
        for acl in current_acls:
            irods_session.acls.set(
                iRODSAccess(
                    access_name="null",
                    user_name=acl.user_name,
                    user_type=acl.user_type,
                    path=collection.path,
                    user_zone=acl.user_zone,
                ),
                recursive=recursive,
            )
        logging.info(f"Discarded existing ACLs on collection {collection.path}")

    for parent_acl in parent_acls:
        # # Skip if the ACL is already set on the collection
        # if irods_session.acls.get(
        #     collection, report_raw_acls=False, user_name=parent_acl.user_name
        # ):
        #     logging.info(
        #         f"Skipping ACL {parent_acl.access_name} for user {parent_acl.user_name} on collection {collection.path} as it is already set."
        #     )
        #     continue


        # Set the ACL on the collection
        irods_session.acls.set(
            iRODSAccess(
                access_name=parent_acl.access_name,
                user_name=parent_acl.user_name,
                user_type=parent_acl.user_type,
                path=collection.path,
                user_zone=parent_acl.user_zone,
            ),
            recursive=recursive,
        )

    return dest_path


@shared_task(name="mango_flow.tasks.noop", bind=True)
def fiber_ingress_noop(self, *args, **kwargs):
    """
    No operation task debugging.
    """
    print(f"fiber_ingress_noop: {args=}, {kwargs=}")
    return args, kwargs
