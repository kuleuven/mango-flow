#!/bin/bash

# options
while getopts "a:q:d:l:h" opt; do
  case $opt in
    a) action="$OPTARG"
    ;;
    q) queue="$OPTARG"
    ;;
    d) directory="$OPTARG"
    ;;
    l) loglevel="$OPTARG"
    ;;
    h) help=1
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
        exit 1
    ;;
  esac
done

if [ $# -eq 0 ] || [ ! -z "$help" ]
  then
    echo "Usage: $0 [-a action] [-q queue] [-d directory] [-l loglevel]"
    echo "Example: $0 -a worker -q celery -d /path/to/directory -l INFO"
    echo "Options:"
    echo "  -a action: The action to perform (e.g., worker, beat, flower)."
    echo "  -q queue: The queue to listen to (default: celery)."
    echo "  -d directory: The directory to change to before running the command."
    echo "  -l loglevel: The log level (default: INFO)."
    exit 0
fi

if [ ! -z "$directory" ]
  then
    cd $directory
fi

exec celery -A plugins.mango_flow.mango_flow_app.mango_flow $action -Q ${queue:-celery} -l ${loglevel:-INFO}
