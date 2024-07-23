#!/bin/bash

#export MANGO_FLOW_GLOBAL_CONFIG=develop/mango_flow.test_paul.yaml
#export API_URL=
#export API_TOKEN=

beat_command=(celery -A plugins.mango_flow.mango_flow_app.mango_flow beat -l INFO)
until "${beat_command[@]}"; do
    echo "Celery beat crashed with exit code $?.  Respawning in 5 seconds.." >&2
    sleep 5
done
