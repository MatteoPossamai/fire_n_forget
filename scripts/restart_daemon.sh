#!/bin/bash -e

THIS_DIR=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")
BASE_DIR="$(dirname "$THIS_DIR")"

python3 "$THIS_DIR/stop_process.py" daemon
"$THIS_DIR/run_daemon.sh"
echo "Restart the client too, so it registers with the new daemon."
