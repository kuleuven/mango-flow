import logging

from celery import shared_task
from irods.access import iRODSAccess
from irods.collection import iRODSCollection
from irods.session import iRODSSession

from .. import MangoFlowTask, get_zone_operator_session, mf_apply_jinja2_template
from ..lib.patterns import mango_flow_regex_search_path
from ..lib.util import read_yaml_from_data_object, get_irods_item

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
    name="mango_flow.tasks.user_management.read_yaml_for_user_management",
    bind=True,
    base=MangoFlowTask,
)
def read_yaml_for_user_management_task(self, path: str, path_regex: str, **kwargs):
    """
    Task to update the iRODS users in one or more groups from a YAML file.
    """
    client_user = kwargs.pop("client_user", None)
    path_info = mango_flow_regex_search_path(path_regex, path)
    zone = (
        path_info["zone"]["value"] if "zone" in path_info else kwargs.pop("zone", None)
    )

    realm = path_info["realm"]["value"] if "realm" in path_info else None

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

        return {"yaml_data": yaml_data, "zone": zone, "realm": realm}
    except Exception as e:
        raise e


# three tasks: one yaml to yaml_data, one yaml_data to list of users (mango or non-mango), and one list of users to added/removed


@shared_task(
    name="mango_flow.tasks.user_management.update_irods_users_groups_from_yaml",
    bind=True,
    base=MangoFlowTask,
)
def update_irods_users_groups_from_yaml_task(
    self, task_input: dict[str : dict | str], **kwargs
):
    """
    Task to update the iRODS users in one or more groups from a YAML file.
    """

    result = {}
    yaml_data = task_input.get("yaml_data", {})

    if len(yaml_data) == 0:
        return task_input
    client_user = kwargs.pop("client_user", None)
    zone = kwargs.pop("zone", task_input.get("zone", None))

    if not zone:
        raise ValueError("Zone is required")

    realm = kwargs.pop("realm", task_input.get("realm", None))

    irods_session = get_zone_operator_session(zone=zone, client_user=client_user)
    for group_name, updated_users in yaml_data.items():
        # Check if the updated users is a list
        if not isinstance(updated_users, list):
            raise ValueError(
                f"Invalid updated users for group {group_name}: {updated_users}"
            )

        # Update the iRODS users in the group
        result[group_name] = update_irods_users_groups(
            irods_session, group_name, updated_users, realm=realm
        )

    return {"result": result, "realm": realm, "zone": zone}


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

    irods_item = get_irods_item(irods_session, dest_path)

    parent_collection = irods_session.collections.get(
        "/".join(dest_path.split("/")[:-1])
    )
    logger.info(
        f"user_management_enforce_acls_parent: parentpath =  {parent_collection.path}"
    )

    parent_acls = irods_session.acls.get(parent_collection, report_raw_acls=True)
    current_acls = irods_session.acls.get(
        irods_item,
        report_raw_acls=True,
    )

    if discard_existing_acls:
        for acl in current_acls:
            irods_session.acls.set(
                iRODSAccess(
                    "null",
                    dest_path,
                    user_name=acl.user_name,
                    user_type=acl.user_type,
                    user_zone=acl.user_zone,
                ),
                recursive=recursive,
                admin=True,
            )
        logger.info(
            f"user_management_enforce_acls_parent: discarded existing ACLs on collection {dest_path.path}"
        )

    for parent_acl in parent_acls:
        irods_session.acls.set(
            iRODSAccess(
                parent_acl.access_name,
                dest_path,
                user_name=parent_acl.user_name,
                user_type=parent_acl.user_type,
                user_zone=parent_acl.user_zone,
            ),
            recursive=recursive,
            admin=True,
        )

    return dest_path


def enforce_acls(
    self,
    irods_session: iRODSSession,
    path: str,
    acls: list[dict],
    recursive: bool = True,
    discard_existing_acls: bool = True,
    handle_missing_user_or_group: str = "raise_error",  # 'raise_error', 'ignore', 'create'
    **kwargs,
):
    """
    Enforce ACLs on a given path. This is especially useful for move operations
    where the parent collections typically has or get the ACLs through inheritance
    """

    irods_item = get_irods_item(irods_session, path)

    if discard_existing_acls:
        current_acls = irods_session.acls.get(
            irods_item,
            report_raw_acls=True,
        )

        for acl in current_acls:
            irods_session.acls.set(
                iRODSAccess(
                    "null",
                    path,
                    user_name=acl.user_name,
                    user_type=acl.user_type,
                    user_zone=acl.user_zone,
                ),
                recursive=recursive,
                admin=True,
            )
        logger.info(
            f"user_management_enforce_acls: discarded existing ACLs on collection {path}"
        )
    # Todo check if the user_name exists
    for acl in acls:
        try:
            irods_session.users.get(acl["user_name"])
        except Exception as e:
            if handle_missing_user_or_group == "raise_error":
                raise ValueError(f"User {acl['user_name']} cannot be found: {e}")
            elif handle_missing_user_or_group == "create":
                # Create the user if it does not exist
                try:
                    irods_session.users.create(
                        acl["user_name"],
                        user_type=acl.get("user_type", "rodsgroup"),
                        user_zone=acl.get("user_zone", irods_session.zone),
                    )
                except Exception as e:
                    raise ValueError(
                        f"Failed to create user {acl['user_name']}: {e}"
                    ) from e
            elif handle_missing_user_or_group == "ignore":
                logger.warning(
                    f"User {acl['user_name']} does not exist in iRODS, ignoring ACL enforcement."
                )
                continue
            else:
                raise ValueError(
                    f"Invalid handle_missing_user_or_group option: {handle_missing_user_or_group}"
                )

        irods_session.acls.set(
            iRODSAccess(
                acl["access_name"],
                path,
                user_name=acl["user_name"],
                user_type=acl.get("user_type", "rodsgroup"),
                user_zone=acl.get("user_zone", irods_session.zone),
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
    recursive = kwargs.pop("recursive", True)

    irods_session: iRODSSession = get_zone_operator_session(
        zone=path.split("/")[1]  # , client_user=client_user
    )

    extracted_dict = mango_flow_regex_search_path(
        mango_regex_pattern=path_regex, path=path
    )

    unprocessed_acls = kwargs.pop("acls", [])
    acls = []
    discard_existing_acls = kwargs.pop("discard_existing_acls", True)
    handle_missing_user_or_group = kwargs.pop(
        "handle_missing_user_or_group", "raise_error"
    )
    if unprocessed_acls:
        for acl in unprocessed_acls:
            acl["user_name"] = mf_apply_jinja2_template(
                acl["user_name"], **extracted_dict
            )
            acls.append(acl)
        logger.warning(
            f"Enforcing ACLs on path {path} with regex {path_regex} and acls: {acls}"
        )
        enforce_acls(
            self,
            irods_session,
            path,
            acls,
            recursive=recursive,
            discard_existing_acls=discard_existing_acls,
            handle_missing_user_or_group=handle_missing_user_or_group,
            **kwargs,
        )
    else:
        logger.warning(
            f"No ACLs found for path {path} with regex {path_regex}. Skipping enforcement."
        )

    return path
