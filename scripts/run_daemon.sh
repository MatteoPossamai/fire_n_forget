#!/bin/bash -e

THIS_DIR=$(dirname -- "$(readlink -f -- "${BASH_SOURCE[0]}")")
BASE_DIR="$(dirname "$THIS_DIR")"

cd "$BASE_DIR"
uv sync
source "$BASE_DIR/.venv/bin/activate"
source "$BASE_DIR/.env"

uv run python -m fire_n_forget.daemon "$@" &
echo "Daemon running, PID: $!"
