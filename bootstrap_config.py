import yaml
import pathlib
import json
import os

def parse_global_config_yaml(global_config_file: pathlib.Path) -> dict|bool:
    # a YAML file can have several "docs"
    try:
        doc_list_iterator = yaml.safe_load_all(global_config_file.read_text())
    except Exception as e:
        # log in dedicated channel?
        return False
    doc_list = [doc for doc in doc_list_iterator]
    mf_zone_config = {doc['zone']: doc for doc in doc_list}
    for zone in mf_zone_config.keys():
        mf_zone_config[zone].pop('zone')
    return mf_zone_config

def parse_beat_flow_item(item_config: dict) -> dict:
    # Simplified: only single well defined config
    # @todo : add more parsing and validation, chaining
    #try:
    if not(item_config.get('trigger', None) and item_config['trigger'].get('beat', None)):
        return False

    beatconfig = item_config['trigger']['beat']
    beatconfig['kwargs']= {'subtree': item_config['match']['subtree']}
    if (flow:=item_config.get('flow', None)) and item_config['flow'].get('mode') == 'single':
        beatconfig['kwargs'] |= {'wf_action': flow['action']['name']}
        beatconfig['kwargs'] |= flow['action']['parameters']
        if client_user :=flow['action'].get('client_user', None):
            beatconfig['kwargs']['client_user'] = client_user
    else:
        return False
    #except Exception as e:
    #    print(e)
    #    return False

    return beatconfig

def get_beat_configs(mango_global_config_file_name):
    mango_global_config_file = pathlib.Path(mango_global_config_file_name)
    zone_configs = parse_global_config_yaml(mango_global_config_file)

    beats = {}
    for zone, zone_config in zone_configs.items():
        for name, itemconfig in zone_config.items():
            if valid_beat_item := parse_beat_flow_item(itemconfig):
                beats[f"{zone}_{name}"]=valid_beat_item
    print(json.dumps(beats, indent=2))
    return beats
    