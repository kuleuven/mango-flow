import celery
import irods.exception
import jinja2
import mango_lib.irods.auth
from flask import Flask
from mango_lib.jinja import MangoJinjaExtension


class MangoFlowTask(celery.Task):
    """Custom implementation of a celery Task class"""

    auto_retry_for = (irods.exception.CAT_CONNECT_ERR, irods.exception.NetworkException)
    retry_backoff = True  # exponential backoff
    retry_backoff_max = 1800  # 30 min is the maximum to wait between retries, but it will try for much longer in total
    # retry_jitter = True  # default, avoid thermonuclear reactions


# ManGO Flow wide operator configuration
# fetched from ManGO Lib which in turn looks also for an ENV variable
# to specify the specific implementation

get_zone_operator_session = mango_lib.irods.auth.get_zone_operator_session


# ManGO Flow wide jinja2 environment
# This environment is used for generating all kinds of strings using the included filters and
# some extra ManGO/iRODS for example to obtain metadata for a certain irods item .

MANGO_FLOW_JINJA2_ENVIRONMENT = jinja2.Environment(
    autoescape=jinja2.select_autoescape(default=False, default_for_string=False),
    trim_blocks=True,
    lstrip_blocks=True,
    extensions=[MangoJinjaExtension],
)


# utility function that uses the dedicated Jinja2 environment to apply a template with the given variables
def mf_apply_jinja2_template(dest_template: str, **variables):
    return MANGO_FLOW_JINJA2_ENVIRONMENT.from_string(dest_template).render(**variables)


def init_app(app: Flask):
    """Initialize the ManGO Flow application with the given Flask app instance."""
    # Import and register blueprints, extensions, or any other initialization code here
    from .flask.report import mango_flow_report_bp

    app.register_blueprint(mango_flow_report_bp)
