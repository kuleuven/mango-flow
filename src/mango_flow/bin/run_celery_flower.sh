#!/bin/bash


celery -A plugins.mango_flow.mango_flow_app.mango_flow flower  --basic-auth="admin:flower,paul:mango" -l INFO
