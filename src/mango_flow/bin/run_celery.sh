#!/bin/bash

# check if file boot_params.sh exists in the same directory as this script
if [ -f "$(dirname "$0")/boot_params.sh" ]; then
  . "$(dirname "$0")/boot_params.sh"
else
  echo "No boot_params.sh found in the same directory as this script."
fi

# cli options
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

if [ $# -eq 0 ] || [ ! -z "$help" ]; then
    echo "Usage: $0 [-a action] [-q queue] [-d directory] [-l loglevel]"
    echo "Example: $0 -a worker -q celery -d /path/to/directory -l INFO"
    echo "Options:"
    echo "  -a action: The action to perform (e.g., worker, beat, flower)."
    echo "  -q queue: The queue to listen to (default: celery)."
    echo "  -d directory: The directory to change to before running the command."
    echo "  -l loglevel: The log level (default: INFO)."
    echo "  -h: Show this help message."
else
  if [ ! -z "$directory" ]; then
      cd $directory
  fi

    # only queue if action is "worker" 
    # The option -n is used to specify the name of the worker, which is important when running multiple workers on the same machine.
    # The %h below is specific for celery and expands to the fully qualified hostname of the machine it runs on
    # The name is here heuristically set to the queue name
  if [ "$action" = "worker" ]; then
    exec celery -A mango_flow.mango_flow_app.mango_flow $action -Q ${queue:-celery} -l ${loglevel:-INFO} -n ${queue:-celery}@%h
  else
    exec celery -A mango_flow.mango_flow_app.mango_flow $action -l ${loglevel:-INFO}
  fi
fi

