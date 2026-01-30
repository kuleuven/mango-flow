from irods.session import iRODSSession
from irods.query import SpecificQuery
from irods.exception import (
    CAT_UNKNOWN_SPECIFIC_QUERY,
    CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME,
    CAT_NO_ROWS_FOUND,
)
from irods.access import iRODSAccess
from irods import MAX_SQL_ROWS

import datetime


class MangoSpecificQuery(SpecificQuery):
    def __init__(self, *args, **kwargs):
        self.max_sql_rows = kwargs.pop("max_sql_rows", MAX_SQL_ROWS)
        super().__init__(*args, **kwargs)

    def execute(self, limit=None, offset=0, options=0, conditions=None):
        if not limit:
            limit = self.max_sql_rows
            print(f"using my limit: {limit}")
        return super().execute(limit, offset, options, conditions)


alias_sql = {
    # find objects thata have metadata and have changes since <t>
    # maybe there will be a use case someday
    "objectsWithMetadataAndChangesSubtree": "SELECT DISTINCT c.coll_name, d.data_name, d.modify_ts "
    "FROM R_COLL_MAIN c, R_DATA_MAIN d, R_OBJT_METAMAP m "
    "where d.coll_id = c.coll_id AND c.coll_name like ? "
    "AND m.object_id = d.data_id "
    "AND (d.modify_ts >= ? OR m.modify_ts >= ? ) ",
    # Look for objects that have changes in metdata since <t>
    "objectsWithChangedMetadataSubtree": "SELECT DISTINCT c.coll_name, d.data_name, m.modify_ts "
    "FROM R_COLL_MAIN c, R_DATA_MAIN d, R_OBJT_METAMAP m "
    "where (d.coll_id = c.coll_id AND c.coll_name like ? "
    "AND m.object_id = d.data_id AND m.modify_ts >= ? )",
    # get a list of groups a user belongs to via the user_name, returns also the user_id along the group ids
    "getUserAndGroupidsForUserName": "select ug.user_id, group_user_id, user_name from R_USER_GROUP ug "
    "inner join R_USER_MAIN u on ug.group_user_id = u.user_id where user_type_name = 'rodsgroup' and ug.user_id = "
    "(select user_id from R_USER_MAIN where user_name = ? and user_type_name != 'rodsgroup')",
    # check if a user has access to a certain object (collection or data object)
    "hasUserIdAccessToAs": "select user_id from R_OBJT_ACCESS where user_id = ? AND object_id = ? and access_type_id >= ?",
    # data objects that are changed since <t>
    "objectsViaModifiedTsSubtree": "select c.coll_name, d.data_name, d.modify_ts FROM R_COLL_MAIN c, R_DATA_MAIN d "
    "where (d.coll_id = c.coll_id AND c.coll_name like ? "
    "AND d.modify_ts >= ? )",
    # collections via modified ts since <t>. This means it has objects added or deleted
    # may be a faster detection in collections with massive amounts of data objects than having to loop over
    # a subtree set
    "collectionsViaModifiedTsSubtree": "select c.coll_name, c.modify_ts FROM R_COLL_MAIN c"
    "where c.colle_name like ? AND c.modify_ts >= ?",
}


def register_specific_query(admin_irods_session: iRODSSession, alias: str, sql: str):
    sq_object = SpecificQuery(admin_irods_session, alias=alias, sql=sql)
    try:
        sq_object.remove()
    except CAT_UNKNOWN_SPECIFIC_QUERY:
        pass
    sq_object.register()


def get_changed_data_objects_in_collection_path(
    admin_irods_session: iRODSSession,
    path: str,
    since: datetime.datetime,
    subtree_search: bool = True,
) -> list:
    """"""
    alias = "objectsWithMetadataAndChangesSubtree"
    path_expression = f"{path}%" if subtree_search else f"{path}"
    since_expression = f"{int(since.timestamp()):011d}"
    and_where_expression = ""
    print((alias, path_expression, since_expression))
    sq_object = MangoSpecificQuery(
        admin_irods_session,
        alias=alias,
        args=[path_expression, since_expression, since_expression],
        max_sql_rows=1000000,
    )
    try:
        results = [f"{row_[0]}/{row_[1]}" for row_ in sq_object]
        return results
    except:
        return []


def get_changed_data_objects_via_metadata_in_collection_path(
    admin_irods_session: iRODSSession,
    path: str,
    since: datetime.datetime,
    subtree_search: bool = True,
) -> list:
    """"""
    alias = "objectsWithChangedMetadataSubtree"
    path_expression = f"{path}%" if subtree_search else f"{path}"
    since_expression = f"{int(since.timestamp()):011d}"
    print((alias, path_expression, since_expression))
    sq_object = MangoSpecificQuery(
        admin_irods_session,
        alias=alias,
        args=[path_expression, since_expression],
        max_sql_rows=1000000,
    )
    try:
        results = [f"{row_[0]}/{row_[1]}" for row_ in sq_object]
        return results
    except:
        return []


def get_changed_data_objects_via_modified_ts_in_collection_path(
    admin_irods_session: iRODSSession,
    path: str,
    since: datetime.datetime,
    subtree_search: bool = True,
) -> list:
    """"""
    alias = "objectsViaModifiedTsSubtree"
    path_expression = f"{path}%" if subtree_search else f"{path}"
    since_expression = f"{int(since.timestamp()):011d}"
    sq_object = MangoSpecificQuery(
        admin_irods_session,
        alias=alias,
        args=[path_expression, since_expression],
        max_sql_rows=1000000,
    )
    try:
        results = [f"{row_[0]}/{row_[1]} ({row_[2]})" for row_ in sq_object]
        return results
    except Exception as e:
        print(e.__dict__)
        print(
            f"crashed {get_changed_data_objects_via_modified_ts_in_collection_path.__name__}"
        )
        return []


def get_user_id_and_group_ids_for_acls_by_user_name(
    admin_irods_session: iRODSSession,
    user_name: str,
) -> list:
    alias = "getUserAndGroupidsForUserName"
    try:
        sq_object = SpecificQuery(admin_irods_session, alias=alias, args=[user_name])
        results = [row for row in sq_object]

        if results:
            user_group_ids = [results[0][0]]
            user_group_ids += [result[1] for result in results]
            return user_group_ids
    except:
        return []


def has_user_access_to(
    admin_irods_session: iRODSSession,
    user_id: int,
    item_id: int,
    access_type: int | str,
) -> bool:
    alias = "hasUserIdAccessToAs"
    if type(access_type) == str:
        access_type = iRODSAccess.codes[access_type]
    sq_object = SpecificQuery(
        admin_irods_session, alias=alias, args=[user_id, item_id, access_type]
    )
    try:
        results = [row for row in sq_object]
        return True
    except CAT_NO_ROWS_FOUND:
        return False
