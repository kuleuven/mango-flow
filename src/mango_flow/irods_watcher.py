# -*- coding: utf-8 -*-
from datetime import datetime
import os

from irods import __version__ as prc_version
from irods.column import Criterion, Like
from irods.models import Collection, DataObject, DataObjectMeta
from irods.query import Query
from irods.session import iRODSSession
from irods.models import (
    Collection,
    DataObject,
    DataObjectMeta,
)
from irods.session import iRODSSession
from irods.query import Query
from irods.column import Criterion, Like
from irods import __version__ as prc_version
from datetime import datetime

MYSQL_IRODS_QUERY_LIMIT = os.getenv("MYSQL_IRODS_QUERY_LIMIT", 20000)


def get_modified_data_object_paths(
    session: iRODSSession,
    subtree: str,
    modified_since: datetime | None = None,
    modified_mode="data_object",
):
    """
    modified_since must be a datetime.datetime object with TZ set to UTC, as the datime values stored in iRODS are always in UTC
    modified_mode : "data_object", "metadata"
    The value "any" will be added later as it requires iRODS >=4.3.2 and PRC >=2.0.2 to be run efficiently
    """

    objects = [Collection.name,DataObject.name]

    filters = []
    # only data objects, also avoid the duplicates in the result
    filters += [Criterion("=", DataObject.replica_number, 0)]
    filters += [Like(Collection.name, f"{subtree}%")]
    if modified_since:
        if modified_mode == "data_object":
            filters += [Criterion(">=", DataObject.modify_time, modified_since)]
        if modified_mode == "metadata":
            filters += [Criterion(">=", DataObjectMeta.modify_time, modified_since)]

    query = Query(session, *objects).filter(*filters).limit(MYSQL_IRODS_QUERY_LIMIT)
    try:
        results= []
        result = query.execute()
        for r in result:
            results.append(f"{r[Collection.name]}/{r[DataObject.name]}")
    except Exception as e:
        results = []
    return results

def search_items_via_metadata(irods_session: iRODSSession, subtree_path = None, collection = None, data_object = None, metadata: list = []) -> list: 
    pass