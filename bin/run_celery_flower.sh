#!/bin/bash

# export MANGO_FLOW_GLOBAL_CONFIG=develop/mango_flow.test_paul.yaml 
# export API_URL=
# export API_TOKEN=
celery -A plugins.mango_flow.mango_flow_app.mango_flow flower  --basic-auth="admin:flower,paul:mango" -l INFO