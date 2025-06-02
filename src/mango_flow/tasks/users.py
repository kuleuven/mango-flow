from .util import read_yaml_from_data_object

from irods.session import iRODSSession
from irods.collection import iRODSCollection
from irods.meta import iRODSMeta
from celery import shared_task
from celery.utils.log import get_task_logger
from plugins.operator import get_zone_operator_session

def update_irods_users_groups(irods_session: iRODSSession, group_name: str,  updated_users: list) -> dict:
    """
    Compare two user lists and return the added and removed users.
    """
    old_users=[]
    # Get the current users in the group
    try:
        group = irods_session.user_groups.get(group_name)
        old_users = [user.name for user in group.members]
    except Exception as e:
        irods_session.user_groups.create(group_name)
    
    added = [user for user in updated_users if user not in old_users]
    removed = [user for user in old_users if user not in updated_users]

    for user in added:
        irods_session.user_groups.add_member(group_name, user)
    for user in removed:
        irods_session.user_groups.remove_member(group_name, user)

    return {'added' : added, 'removed': removed}


# @shared_task(name="mango_flow.tasks.users.update_irods_users_groups", bind=True)
# def update_irods_users_groups_task(self, group_name: str, updated_users: list, **kwargs):
#     """
#     Task to update the iRODS users in a group.
#     """
#     client_user = kwargs.pop("client_user", None)

#     zone = kwargs.pop("zone", None)
#     if not zone:
#         raise ValueError("Zone is required")
    
#     irods_session = get_zone_operator_session(zone=zone, client_user=client_user)
    
#     try:
#         result = update_irods_users_groups(irods_session, group_name, updated_users)
#         return result
#     # Handle specific exceptions from irods and report them
#     except Exception as e:
#         raise e
    

@shared_task(name="mango_flow.tasks.users.update_irods_users_groups_from_yaml", bind=True)
def update_irods_users_groups_from_yaml_task(self, path: str, **kwargs):
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

        # Check if the keys have the expected prefix
        if not (realm := kwargs.pop("realm", None)):
            # get it from the 4th position in the path
            realm = path.split("/")[4]
        
        yaml_data = {(k if k.startswith(realm) else f"{realm}_{k}"): v for k, v in yaml_data.items()
        }

        result = {}
        for group_name, updated_users in yaml_data.items():
            # Check if the group name is valid
            if not group_name.startswith(realm):
                raise ValueError(f"Invalid group name: {group_name}")
            # Check if the updated users is a list
            if not isinstance(updated_users, list):
                raise ValueError(f"Invalid updated users for group {group_name}: {updated_users}")
            
            # Update the iRODS users in the group
            result[group_name] = update_irods_users_groups(irods_session, group_name, updated_users)  

    
        return result
    except Exception as e:
        raise e
    

