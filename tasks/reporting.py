from celery import shared_task
from irods.session import iRODSSession
from irods.exception import DataObjectDoesNotExist
import json
import pathlib
from plugins.operator import get_zone_operator_session

# @shared_task(name="mango_flow.irods.report.path.results.json", bind=True)
# def aggregate_path_results_json(
#     *results, path=None, irods_path=None, report_file="mango_flow_report.json"
# ):
#     """
#     Append path *results to a json file
#     results are a list of dicts containing dict.keys() = ("path", "status", "time", "next")
#     """
#     pass


@shared_task(name="mango_flow.irods.report.path.results.csv", bind=True)
def aggregate_path_results_table(
    *results,
    report_file="mango_flow_report.csv",
    sep=";",
    irods_path="",
    client_user=None,
    save_locally=False,
):
    """Append path *results to a .?sv file.

    The results (collection of return statements of the header actions that this is chained to)
    should be dictionaries in which the keys are the column names of the tabular report.

    Args:
        report_file (str, optional): Filename for the reports file. Defaults to "mango_flow_report.csv".
        sep (str, optional): Separator for the columns. Defaults to ";".
        irods_path (str, optional): Absolute path of the iRODS collection to store the report in. Defaults to "".
        client_user (str, optional): User name to save the logs in iRODS. Defaults to None, in which case the logs
          are not stored in iRODS.
        save_locally (bool, optional): Whether the reports should also be saved locally. Defaults to False.

    Returns:
        str: Whether dictionaries were found (and thus the log was populated) or not.
    """
    dicts = [result for result in results[1] if type(result) == dict]
    if len(dicts) == 0:
        return "No dicts found to log!"
    colnames = sep.join(str(i) for i in dicts[0].keys()) + "\n"
    other_lines = "\n".join(
        sep.join(str(i) for i in result.values()) for result in dicts
    )
    if save_locally:
        local_path = pathlib.Path("/", "tmp", report_file)
        local_path_exists = local_path.exists()
        with local_path.open("a") as f:
            if not local_path_exists:
                f.write(colnames)
            f.write(other_lines)
    if client_user is not None:
        irods_report_path = pathlib.Path(irods_path, report_file)
        irods_report_path_as_str = str(irods_report_path)
        zone = irods_report_path.parts[1]
        irods_session = get_zone_operator_session(zone, client_user=client_user)
        if irods_session.data_objects.exists(irods_report_path_as_str):
            obj = irods_session.data_objects.get(irods_report_path_as_str)
            print(f"{irods_report_path} exists!")
            with obj.open("r+") as f:
                f.seek(0, 2)
                f.write(other_lines.encode())
        else:
            print(f"Creating {irods_report_path}!")
            obj = irods_session.data_objects.create(irods_report_path_as_str)
            with obj.open("w") as f:
                f.write(colnames.encode())
                f.write(other_lines.encode())
    else:
        "No client user provided"
    return "Log has been populated!"


@shared_task(name="mango_flow.irods.report.path.results.test")
def aggregate_path_results_test(*results, client_user=None):
    pathlib.Path("/tmp/mango_flow_results.log").write_text(
        "\n".join([str(result) for result in results])
    )
    return "Houston, this is just a test!"
