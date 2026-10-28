#!/bin/bash -e

THIS_DIR=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")
BASE_DIR="$(dirname "$THIS_DIR")"

python3 "$THIS_DIR/stop_process.py" client
"$THIS_DIR/run_client.sh"
