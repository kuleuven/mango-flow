#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit
fi

# export MANGO_FLOW_GLOBAL_CONFIG=develop/mango_flow.test_paul.yaml 
# export API_URL=
# export API_TOKEN=
celery -A plugins.mango_flow.mango_flow_app.mango_flow $1 -l INFO