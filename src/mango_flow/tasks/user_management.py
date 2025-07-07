import logging

from celery import shared_task
from celery.utils.log import get_task_logger
from irods.access import iRODSAccess
from irods.collection import iRODSCollection
from irods.session import iRODSSession
from plugins.operator import get_zone_operator_session

from .util import read_yaml_from_data_object
from ..lib.patterns import mango_flow_regex_search_path

"""Two possible approaches:

- ManGO approach chains three tasks:
    1. mango_flow.tasks.users_management.read_yaml_for_user_management
    2. mango_flow.tasks.mango_user_management.update_mango_users_groups_from_yaml
    3. mango_flow.tasks.user_management.update_irods_users_groups_from_yaml
    
- Other installations skip task number 2

"""

logger = logging.getLogger(__name__)


def update_irods_users_groups(
    irods_session: iRODSSession, group_name: str, updated_users: list, realm: str = None
) -> dict:
    """
    Compare two user lists and return the added and removed users.
    """
    old_users = []
    # Get the current users in the group
    try:
        group = irods_session.groups.get(group_name)
        old_users = [user.name for user in group.members]
    except Exception:
        new_group = irods_session.groups.create(group_name)
        if realm is not None:
            new_group.metadata.add("mg.realm", realm)

    added = [user for user in updated_users if user not in old_users]
    removed = [user for user in old_users if user not in updated_users]

    for user in added:
        irods_session.groups.addmember(group_name, user)
    for user in removed:
        irods_session.groups.removemember(group_name, user)

    return {"added": added, "removed": removed}


@shared_task(
    name="mango_flow.tasks.users_management.read_yaml_for_user_management", bind=True
)
def read_yaml_for_user_management_task(self, path: str, **kwargs):
    """
    Task to update the iRODS users in one or more groups from a YAML file.
    """
    client_user = kwargs.pop("client_user", None)
    zone = kwargs.pop("zone", None)

    if not zone:
        raise ValueError("Zone is required")

    irods_session = get_zone_operator_session(zone=zone, client_user=client_user)

    try:
        yaml_data = read_yaml_from_data_object(path, irods_session)
        if not yaml_data:
            raise ValueError("YAML data is empty or invalid")

        # loop over gorups
        # flattening as long we have a tree
        def flatten_yaml_dict(yaml_dict):
            flat_dict = {}
            for key, value in yaml_dict.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in flatten_yaml_dict(value).items():
                        flat_dict[f"{key}_{sub_key}"] = sub_value
                else:
                    flat_dict[key] = value
            return flat_dict

        yaml_data = flatten_yaml_dict(yaml_data)

        return yaml_data
    except Exception as e:
        raise e


# three tasks: one yaml to yaml_data, one yaml_data to list of users (mango or non-mango), and one list of users to added/removed


@shared_task(
    name="mango_flow.tasks.user_management.update_irods_users_groups_from_yaml",
    bind=True,
)
def update_irods_users_groups_from_yaml_task(self, yaml_data: dict, **kwargs):
    """
    Task to update the iRODS users in one or more groups from a YAML file.
    """

    result = {}

    if len(yaml_data) == 0:
        return result
    client_user = kwargs.pop("client_user", None)
    zone = kwargs.pop("zone", None)

    if not zone:
        raise ValueError("Zone is required")

    irods_session = get_zone_operator_session(zone=zone, client_user=client_user)
    for group_name, updated_users in yaml_data.items():
        # Check if the updated users is a list
        if not isinstance(updated_users, list):
            raise ValueError(
                f"Invalid updated users for group {group_name}: {updated_users}"
            )

        # Update the iRODS users in the group
        result[group_name] = update_irods_users_groups(
            irods_session, group_name, updated_users, realm=kwargs.pop("realm", None)
        )

    return result


@shared_task(name="mango_flow.tasks.user_management.enfore_acls_parent", bind=True)
def user_management_enforce_acls_parent(self, dest_path: str, **kwargs):
    """
    Enforce ACLs on the parent collection of the destination path. This is especially useful for move operations
    where the parent collections typically has or get the ACLs through inheritance

    """
    logger.info(f"user_management_enforce_acls_parent: {dest_path=}, {kwargs=}")
    client_user = kwargs.pop("client_user", None)

    logger.info(f"user_management_enforce_acls_parentt: {client_user=}")
    recursive = kwargs.pop("recursive", True)
    logger.info(f"user_management_enforce_acls_parent: {recursive=}")
    discard_existing_acls = kwargs.pop("discard_existing_acls", False)

    irods_session: iRODSSession = get_zone_operator_session(
        zone=dest_path.split("/")[1]  # , client_user=client_user
    )

    collection: iRODSCollection = irods_session.collections.get(dest_path)

    parent_collection = irods_session.collections.get(
        "/".join(dest_path.split("/")[:-1])
    )
    logger.info(
        f"user_management_enforce_acls_parent: parentpath =  {parent_collection.path}"
    )

    parent_acls = irods_session.acls.get(parent_collection, report_raw_acls=True)
    current_acls = irods_session.acls.get(
        collection,
        report_raw_acls=True,
    )

    if discard_existing_acls:
        for acl in current_acls:
            irods_session.acls.set(
                iRODSAccess(
                    "null",
                    collection.path,
                    user_name=acl.user_name,
                    user_type=acl.user_type,
                    user_zone=acl.user_zone,
                ),
                recursive=recursive,
                admin=True,
            )
        logger.info(
            f"user_management_enforce_acls_parent: discarded existing ACLs on collection {collection.path}"
        )

    for parent_acl in parent_acls:
        irods_session.acls.set(
            iRODSAccess(
                parent_acl.access_name,
                collection.path,
                user_name=parent_acl.user_name,
                user_type=parent_acl.user_type,
                user_zone=parent_acl.user_zone,
            ),
            recursive=recursive,
            admin=True,
        )

    return dest_path


def enforce_acls(self, irods_session: iRODSSession, path: str, acls: list[dict], recursive: bool = True):
    """
    Enforce ACLs on a given path. This is especially useful for move operations
    where the parent collections typically has or get the ACLs through inheritance
    """
    # Todo check if the user_name exists
    for acl in acls:
        irods_session.acls.set(
            iRODSAccess(
                acl["access_name"],
                path,
                user_name=acl["user_name"],
                user_type=acl.get("user_type", "rodsgroup"),
                user_zone=acl.get("user_zone", irods_session.zone)
            ),
            recursive=recursive,
            admin=True,
        )


@shared_task(name="mango_flow.tasks.user_management.enforce_acls", bind=True)
def user_management_enforce_acls(self, path: str, path_regex: str, **kwargs):
    """
    Enforce ACLs on a given path using patterns
    """
    logger.info(f"user_management_enforce_acls: {path=}, {kwargs=}")
    #client_user = kwargs.pop("client_user", None)
    recursive = kwargs.pop("recursive", True)

    irods_session: iRODSSession = get_zone_operator_session(
        zone=dest_path.split("/")[1]  # , client_user=client_user
    )

    collection = irods_session.collections.get(dest_path)

    acls = kwargs.pop("acls", [])
    enforce_acls(self, irods_session, collection.path, acls, recursive=recursive, **kwargs)

    return path