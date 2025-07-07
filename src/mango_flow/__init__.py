import os
from importlib import import_module

import celery
import irods.exception
import jinja2
from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.meta import iRODSMeta


class MangoFlowTask(celery.Task):
    """Custom implementation of a celery Task class"""

    auto_retry_for = (irods.exception.CAT_CONNECT_ERR, irods.exception.NetworkException)
    retry_backoff = True  # exponential backoff
    retry_backoff_max = 1800  # 30 min is the maximum to wait between retries, but it will try for much longer in total
    # retry_jitter = True  # default, avoid thermonuclear reactions


# ManGO Flow wide operator configuration
zone_operator_module = import_module(
    os.getenv("IRODS_ZONE_OPERATOR_MODULE", "plugins.operator")
)
get_zone_operator_session = zone_operator_module.get_zone_operator_session


# ManGO Flow wide jinja2 environment
# This environment is used for generating all kinds of strings using the included filters and
# some extra ManGO/iRODS for example to obtain metadata for a certain irods item .

MANGO_FLOW_JINJA2_ENVIRONMENT = jinja2.Environment(
    autoescape=jinja2.select_autoescape(default=False, default_for_string=False),
    trim_blocks=True,
    lstrip_blocks=True,
)


# utility function that uses the dedicated Jinja2 environment to apply a template with the given variables
def mf_apply_jinja2_template(dest_template: str, **variables):
    return MANGO_FLOW_JINJA2_ENVIRONMENT.from_string(dest_template).render(**variables)


# custom Jinja2 filters


def get_one_irods_metadata(
    irods_item: iRODSDataObject | iRODSCollection, metadata_name, default_value=None
):
    try:
        avu: iRODSMeta = irods_item.metadata.get_one(metadata_name)
        return avu.value if avu else default_value
    except Exception:
        return default_value  # iRODSMeta(metadata_name, default_value)


MANGO_FLOW_JINJA2_ENVIRONMENT.filters["get_one_irods_metadata"] = get_one_irods_metadata
