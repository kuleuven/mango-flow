import json
import pathlib

import yaml


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

def parse_flow_item(item_config: dict) -> dict:
    #try:
    beatconfig = item_config['trigger']['beat']
    beatconfig['kwargs']= {'subtree': item_config['match']['subtree']}
    if (flow:=item_config.get('flow', None)) and item_config['flow'].get('mode') == 'single':
        beatconfig['kwargs'] |= {'wf_action': flow['action']['name']}
        beatconfig['kwargs'] |= item_config['flow']['action']['parameters']
    else:
        print("help, something is not flowing")
    #except Exception as e:
    #    print(e)
    #    return False

    return beatconfig



    

test_file = pathlib.Path('mango_flow.test_paul.yaml')
zone_configs = parse_global_config_yaml(test_file)
#print(json.dumps(zone_configs, indent=2))



beats = {}
for zone, zone_config in zone_configs.items():
    for name, itemconfig in zone_config.items():
        beats[f"{zone}_{name}"]=parse_flow_item(itemconfig)
        #print(f"For zone {zone} and name {name}:")
        #print(json.dumps(itemconfig, indent=2))


print(json.dumps(beats, indent=2))

# zone_configs = {
#     zonedoc['zone']: zone_doc 

# }

# with open('mango_flow.example.yaml', 'r') as testfile:
#     configs = yaml.load_all(testfile)
#     print(yaml.dump(configs))
