from celery import shared_task
from irods.session import iRODSSession
import json
import pathlib


# @shared_task(name="mango_flow.irods.report.path.results.json", bind=True)
# def aggregate_path_results_json(
#     *results, path=None, irods_path=None, report_file="mango_flow_report.json"
# ):
#     """
#     Append path *results to a json file
#     results are a list of dicts containing dict.keys() = ("path", "status", "time", "next")
#     """
#     pass


# @shared_task(name="mango_flow.irods.report.path.results.csv", bind=True)
# def aggregate_path_results_table(
#     *results, path=None, irods_path=None, report_file="mango_flow_report.csv", sep=";"
# ):
#     """
#     Append path *results to a .?sv file

#     results are a list of dicts containing dict.keys() = ("path", "status", "time", "next")
#     """
#     pass


@shared_task(name="mango_flow.irods.report.path.results.test")
def aggregate_path_results_test(*results, client_user = None):
    pathlib.Path("/tmp/mango_flow_results.log").write_text(
        "\n".join([str(result) for result in results])
    )
    return "Houston, this is just a test!"
