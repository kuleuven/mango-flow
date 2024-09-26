#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit
fi

if [ ! -z "$2" ]
  then
    cd $2
fi

exec celery -A plugins.mango_flow.mango_flow_app.mango_flow $1 -l INFO