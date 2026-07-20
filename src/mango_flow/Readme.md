# Mango Flow

## Installation for development

### Dependencies

redis server, python celery and friends

#### installation of redis server in Ubuntu

`$ sudo apt install redis-server`

#### redis commander (utility to have a GUI/web access to Redis)

`$ npm install redis-commander`

Redis commander listens by default on port 8081 

http://localhost:8081

#### celery and dependencies

In your Python venv:

`$ pip install -r requirements-mango-flow.txt`

### Obtain an API token with admin privileges 

To do so, run:  
`$ data-platform-cli token --sudo --tier <t,q,p>`

This can be ran on your WSL if you have data-platform-cli installed, otherwise you can run it on NX.  

### copy the various startup scripts from src/plugins/mango_flow/bin and add the environment variables:

Scripts
```
run_celery.sh
run_celery_beat.sh
run_celery_flower.sh

```

Environment variables

```bash
export MANGO_FLOW_GLOBAL_CONFIG=develop/<your-mango-flow-config>.yaml 
export API_URL=https://icts-p-coz-data-platform-api.cloud.icts.kuleuven.be 
    # https://icts-q-coz-data-platform-api.cloud.q.icts.kuleuven.be for quality,
    # https://icts-t-coz-data-platform-api.cloud.t.icts.kuleuven.be for test
export API_TOKEN=<api token obtained from NX>
```

### Run redis   

sudo systemctl start redis  

### Run the 3 scripts above in dedicated shells

And watch the ouput for errors

### reloading configs

If you change anything in a task code, you will need to restart the worker. If you change anything in the yaml config file, you need also to delete the entire "tree" for "redbeat" in the Redis database. This can be done easily in redis-commander web interface

Then stop and start the beat server again

### Example yaml file

The exmple below shows the two types of flows: "single" and "chain"

```yaml
u0123318_test_mf_ingest_copy:
  trigger:
    beat:
      task: 'mango_flow.monitor.irods.subtree'
      schedule: 30
      client_user: u0123318
  match:
    subtree: '/set/home/u0123318/test_mf/ingest/copy'
  flow:
    mode: single
    action:
      name: 'mango_flow.irods.ingest_copy_data_object'
      parameters:
        path_regex: '/set/home/u0123318/test_mf/ingest/copy/{{dataobject}}'
        #dst_template: '/set/home/u0123318/test_mf/final/images/{{dataobject}}'
        dst_template: '/ghum/home/datateam_ghum/test_paul/mango_flow/final/{{dataobject}}'
        metadata: all

u0123318_test_chain:
  trigger: 
    beat:
      task: 'mango_flow.monitor.irods.subtree'
      schedule: 60
      client_user: u0123318
  match:
    subtree: '/set/home/u0123318/test_mf/ingest/move'
  flow:
    mode: chain
    action:
    - name: 'mango_flow.irods.ingest_move_data_object'
      parameters:
        # client_user: u0123318 inherited if set with trigger
        path_regex: '/set/home/u0123318/test_mf/ingest/move/{{dataobject}}'
        dst_template: '/set/home/u0123318/test_mf/final/move/{{dataobject}}'
    - name: 'mango_flow.irods.extract_metadata_marked_text_file_header'
      parameters:
        # client_user: u0123318 inherited if set with trigger
        path_regex: '[^/]+\.txt$' # only .txt files

```



