import os.path
import re
from datetime import datetime
from pathlib import Path

from celery import shared_task
from plugins.operator import get_zone_operator_session
from .basic_operations import check_file_age

from plugins.mango_flow import MangoFlowTask


def neutralize(string):
    return string.replace("-", "_").lower()


class PatternSet:
    def __init__(self, project_path):
        self.project = Path(*list(project_path.parts[:4]))
        self.pattern_upload = (
            neutralize(str(self.project)) + r"/(?P<sitecollection>[a-z_]+)/upload.*/"
        )

        self.sites_get_mapping = {
            "c1": "emory",
            "c2": "kuleuven",
            "c3": "radboud",
            "c4": "oregon",
            "c5": "sydney",
            "c6": "tel_aviv",
            "c7": "waterloo",
        }
        self.rochester_name = "rochester"
        self.pattern_pre = r"(?P<site>c\d)_(?P<sub>sub\d{3})_(?P<group>fr|nf)_"
        self.pattern_with_irods_session = self.pattern_pre + r"(?P<ses>ses0[12])_"
        self.pattern_long_middle = (
            self.pattern_with_irods_session
            + r"(?P<medstatus>on|off)_(?P<trial>[sd]t(walk|turn|shuf|door|carr|agil)|(vo|cu)stop)(?P<retr>_retr\d+)?"
        )
        self.patterns = {
            "h5": self.pattern_upload
            + r"(?P<timestamp>\d{8}_\d{6})_"
            + self.pattern_long_middle
            + r"\.(?P<ext>h5)$",
            "sensor_csv": self.pattern_upload
            + self.pattern_long_middle
            + r"\.(?P<ext>csv)$",
            "long": self.pattern_upload
            + self.pattern_long_middle
            + r"_(?P<plane>front|sagit)_?(?P<blur>(un)?blur)?\.(?P<ext>mp4|eaf|pfsx|csv)$",
            "moca": self.pattern_upload
            + self.pattern_with_irods_session
            + r"moca.(?P<ext>pdf)$",
            "monitor": self.pattern_upload
            + self.pattern_pre
            + r"7day(_activitydiary)?.(?P<ext>cwa|csv|pdf)$",
            "interview": self.pattern_upload
            + self.pattern_pre
            + r"_interview_(?P<initials>[a-z]+)_(?P<type>mindget_map|transcript)\.(?P<ext>pdf|xmind)$",
        }

    def match_pattern(self, filename):
        """Match a filename to a pattern

        Args:
            filename (str): Normalized filename

        Returns:
            tuple: First element is the pattern if there was a match, False if something is wrong.
                Second element is the match groups if there was a match "error message" otherwise.
        """

        matched = False
        for pattern_name, pattern in self.patterns.items():
            if re.match(pattern, filename):
                filename_groups = re.match(pattern, filename).groupdict()
                matched = True
                break

        if not matched:
            return (False, "No acceptable pattern was matched.")

        if (
            filename_groups["sitecollection"] == self.rochester_name
            and pattern_name != "interview"
        ):
            return (False, "Rochester site files should follow the interview pattern.")
        return (pattern_name, filename_groups)

    def reject_outside_of_project(self, object_path):
        return re.match(self.pattern_upload, neutralize(object_path))


def validate_filename(filename, patternset):
    """Validate a filename against different rules.

    Args:
        filename (str): Full path to the file in an "upload" subcollection.
        patternset (fogcoa.Patternset): Patterns to match against

    Returns:
        tuple: First element is True if the filename is valid, False otherwise.
               Second element is a nested dictionary with filename elements and the
               pattern if the filename is valid, an "error message" otherwise.
    """
    # letters may be capitalized or not in the original name
    # underscore and hyphen are acceptable but hyphens should become underscores
    filename = neutralize(filename)

    matching = patternset.match_pattern(filename)
    if matching[0]:
        pattern, filename_groups = matching
    else:
        return matching

    valid_trial_values = [
        "stwalk",
        "dtwalk",
        "stturn",
        "dtturn",
        "stshuf",
        "stdoor",
        "stcarr",
        "stagil",
        "vostop",
        "custop",
    ]

    # match numeric value in site with the original site (check parent collection)
    if filename_groups["site"] not in patternset.sites_get_mapping:
        return (False, f"Site prefix {filename_groups['site'].upper()} is not valid.")
    if (
        filename_groups["sitecollection"] != patternset.rochester_name
        and filename_groups["sitecollection"]
        != patternset.sites_get_mapping[filename_groups["site"]]
    ):
        return (
            False,
            f"Site prefix {filename_groups['site'].upper()} does not match source ({filename_groups['sitecollection']}).",
        )

    if pattern == "long":
        # if file extension is mp4, there should be sagit/front AND blur
        if filename_groups["ext"] == "mp4" and filename_groups["blur"] == "unblur":
            return (
                False,
                "This mp4 file is unblurred and was deleted.",
            )
        filename_groups["blur"] = "blur"

        if filename_groups["trial"] not in valid_trial_values:
            return (False, f"{filename_groups['trial']} is not a valid trial value.")

    elif pattern == "h5":
        try:
            timestamp_as_date = datetime.strptime(
                filename_groups["timestamp"], "%Y%m%d_%H%M%S"
            )
            filename_groups["timestamp"] = timestamp_as_date.isoformat()
        except:
            return (False, "The timestamp is not in the correct format.")

    elif pattern == "monitor":
        ext_pdf = filename_groups["ext"] == "pdf"
        act_diary = "activitydiary" in filename
        if ext_pdf and not act_diary:
            return (
                False,
                "Monitor files with pdf extension should be activity diaries.",
            )
        if not ext_pdf and act_diary:
            return (False, "Activity diaries should have pdf extension.")

    # Organize elements to rebuild path
    compulsory_elements = ["sitecollection", "site", "sub", "group", "ext"]
    return_dict = {
        "common": {
            k: v for k, v in filename_groups.items() if k in compulsory_elements
        },
        "specific": {
            k: v for k, v in filename_groups.items() if not k in compulsory_elements
        },
        "pattern": pattern,
    }

    return (True, return_dict)


def choose_source_collection(sitecollection, site, ext, pattern, patternset):
    """Select subcollection under project collection.

    Args:
        sitecollection (str): Name of the site (subcollection under project collection)
        site (str): Site code (prefix of the file)
        ext (str): Extension
        pattern (str): Regular expression pattern that the filename matched to
        specific (dict): Dictionary with other values, to sort interview files.

    Returns:
        pathlib.Path: Subcollection for the destination of the file.
    """
    if sitecollection == patternset.rochester_name:
        home_collection = (
            patternset.project
            / patternset.sites_get_mapping[site]
            / f"{site.upper()}_interview"
        )
        return (
            home_collection / "source_data"
            if ext == "xmind"
            else home_collection / "derived_data"
        )

    home_collection = patternset.project / sitecollection / "structured_data"

    if pattern in ["moca", "h5"] or (pattern == "monitor" and ext != "csv"):
        return home_collection / "source_data"
    return home_collection / "derived_data"


def build_destination_path(validated_filename, patternset):
    """Build the destination of the file

    Args:
        validated_filename (dict): Dictionary with information to rebuild the destination path

    Returns:
        pathlib.Path: Destination path
    """
    common, specific, pattern = validated_filename.values()
    sitecollection, site, sub, group, ext = common.values()
    site = site.upper()
    full_sub = f"{site}_{sub}_{group.upper()}"
    collection = (
        choose_source_collection(sitecollection, site, ext, pattern, patternset)
        / full_sub
    )

    if pattern == "interview":
        filename = f"{full_sub}_interview_{specific['initials'].upper()}_{specific['type']}.{ext}"
    elif pattern == "moca":
        collection = collection / specific["ses"]
        filename = f"{full_sub}_{specific['ses']}_moca.pdf"
    elif pattern == "monitor":
        collection = collection / "7-day_monitor"
        filename = f"{full_sub}_7day{'_activitydiary' if ext == 'pdf' else ''}.{ext}"
    else:
        collection = collection / specific["ses"] / specific["medstatus"]
        retr = (
            specific["retr"].replace("_", "-") if specific["retr"] is not None else ""
        )
        filename_end = (
            f"_{specific['plane']}_{specific['blur']}" if "plane" in specific else ""
        )
        filename = f"{full_sub}_{specific['ses']}_{specific['medstatus']}_{specific['trial']}{retr}{filename_end}.{ext}"

    return collection / filename


@shared_task(
    name="mango_flow.irods.validate_fogcoa_files", bind=True, base=MangoFlowTask
)
def wrap_validate_fogcoa_files(self, path, path_regex, client_user):
    if not re.match(path_regex, path):
        return path
    path_as_path = Path(path)
    zone = path_as_path.parts[1]
    irods_session = get_zone_operator_session(zone, client_user=client_user)

    irods_file = irods_session.data_objects.get(path)
    file_age = check_file_age(irods_file)
    if file_age < 60:
        return f"{path} is only {file_age} seconds old."

    patternset = PatternSet(path_as_path)
    validated, result = validate_filename(path, patternset)
    failing_dest = str(
        patternset.project
        / path_as_path.parts[4]  # university
        / "upload"
        / path_as_path.name
    )

    log_data = {
        "timestamp": str(datetime.now()),
        "original_path": path,
        "destination_path": failing_dest,
        "message": result,
        "succeeded": False,
    }

    # deal with bad filenames
    if not validated:
        if path.endswith("_unblur.mp4"):
            irods_session.data_objects.unlink(path, force=True)
            failing_dest = "NA"
        elif path != failing_dest:
            if irods_session.data_objects.exists(failing_dest):
                log_data["destination_path"] = path
            else:
                irods_session.data_objects.move(path, failing_dest)
        return log_data

    destination_path = build_destination_path(result, patternset)
    dest = str(destination_path)
    collection = str(destination_path.parents[0])
    if not irods_session.collections.exists(collection):
        try:
            irods_session.collections.create(collection, recurse=True)
        except:
            print(f"{collection} is already being created by another process :)")

    # deal with repeated destination names
    if irods_session.data_objects.exists(dest):
        if path != failing_dest:
            if irods_session.data_objects.exists(failing_dest):
                log_data["destination_path"] = path
            else:
                irods_session.data_objects.move(path, failing_dest)
        log_data["message"] = f"Destination path '{dest}' already exists."
        return log_data

    # if everything went smoothly:
    irods_session.data_objects.move(path, dest)
    comments = []
    if "timestamp" in result["specific"]:
        irods_session.data_objects.get(dest).metadata.add(
            "recording_time", result["specific"]["timestamp"]
        )
        comments.append("Timestamp added as metadata")
    if path_as_path.name != destination_path.name:
        comments.append("Filename changed")
    if result["common"]["sub"].endswith("02") and result["common"]["group"] == "nf":
        comments.append("NF with ses02")
    log_data["destination_path"] = dest
    log_data["message"] = ", ".join(comments)
    log_data["succeeded"] = True
    return log_data
