#!/bin/bash


beat_command=(celery -A plugins.mango_flow.mango_flow_app.mango_flow beat -l INFO)
until "${beat_command[@]}"; do
    echo "Celery beat crashed with exit code $?.  Respawning in 5 seconds.." >&2
    sleep 5
done
