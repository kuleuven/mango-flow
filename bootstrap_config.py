import yaml
import pathlib
import json


def parse_global_config_yaml(global_config_file: pathlib.Path) -> dict | bool:
    # a YAML file can have several "docs"
    try:
        doc_list_iterator = yaml.safe_load_all(global_config_file.read_text())
    except Exception as e:
        # log in dedicated channel?
        return False
    doc_list = [doc for doc in doc_list_iterator]
    mf_zone_config = {doc["zone"]: doc for doc in doc_list}
    for zone in mf_zone_config.keys():
        mf_zone_config[zone].pop("zone")
    return mf_zone_config


def parse_beat_flow_item(item_config: dict) -> dict:
    # Simplified: only single well defined config
    # @todo : add more parsing and validation, chaining
    # try:
    if not (
        item_config.get("trigger", None) and item_config["trigger"].get("beat", None)
    ):
        return False

    client_user = item_config["trigger"]["beat"].pop("client_user", None)
    path_input_mode = item_config["trigger"]["beat"].pop("path_input_mode", "single") #multi # single is the default, multi means all changed paths are passed in 1 go  into the flow entry point

    beatconfig = item_config["trigger"]["beat"]  # delivers task name and schedule

    beatconfig["kwargs"] = {
        "subtree": item_config["match"]["subtree"],
        "client_user": client_user,
        "path_input_mode": path_input_mode,
    }

    if (flow := item_config.get("flow", None)) and item_config["flow"].get(
        "mode"
    ) == "single":
        beatconfig["kwargs"] |= {"wf_action": flow["action"]["name"]}
        if client_user and (not "client_user" in flow["action"]["parameters"].keys()):
            flow["action"]["parameters"]["client_user"] = client_user
        beatconfig["kwargs"] |= {"kwargs": flow["action"]["parameters"]}
        return beatconfig

    if (flow := item_config.get("flow", None)) and item_config["flow"].get(
        "mode"
    ) == "chain":
        # propagate/inject client_user if needed
        for task in flow["action"]:
            if client_user and "client_user" not in task["parameters"]:
                task["parameters"]["client_user"] = client_user
        # now call the task chain
        beatconfig["kwargs"] |= {
            "wf_action": "mango_flow.irods.chain.path",
            "kwargs": {"tasks": flow["action"]},
        }
        return beatconfig
    
    if (flow := item_config.get("flow", None)) and item_config["flow"].get(
        "mode"
    ) == "map":
        # propagate/inject client_user if needed
        for task in flow["header_actions"]:
            if client_user and "client_user" not in task["parameters"]:
                task["parameters"]["client_user"] = client_user
        for task in flow["aggregate_actions"]:
            if client_user and "client_user" not in task["parameters"]:
                task["parameters"]["client_user"] = client_user
        # now call the task chain
        beatconfig["kwargs"] |= {
            "wf_action": "mango_flow.irods.map.chain.path",
            "kwargs": {"header_tasks": flow["header_actions"], "aggregate_tasks": flow["aggregate_actions"]},
        }
        return beatconfig

    return False


def get_beat_configs_from_global_file(mango_global_config_file_name):
    mango_global_config_file = pathlib.Path(mango_global_config_file_name)
    zone_configs = parse_global_config_yaml(mango_global_config_file)

    beats = {}
    for zone, zone_config in zone_configs.items():
        for name, itemconfig in zone_config.items():
            if valid_beat_item := parse_beat_flow_item(itemconfig):
                beats[f"{zone}_{name}"] = valid_beat_item
    print(json.dumps(beats, indent=2))
    return beats
