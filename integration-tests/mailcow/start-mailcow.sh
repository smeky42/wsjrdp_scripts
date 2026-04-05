#!/usr/bin/env bash

set -euo pipefail

SELFDIR="$(dirname "$(readlink -f "$BASH_SOURCE")")"
ROOTDIR="$(cd "$SELFDIR" && cd .. && pwd)"
MAILCOW_DICKERIZED_DIR="${ROOTDIR}/.mailcow-dockerized"

# Define call and error functions
if [ -t 0 ] && [ -t 1 ]; then
    msg_pattern='\e[1m\e[97m%s\e[0m'
    call_pattern='\e[1m\e[35m%s\e[0m'
    err_pattern='\e[1m\e[31m%s\e[0m'
else msg_pattern='%s'; call_pattern='%s'; err_pattern='%s'; fi
msg() { printf "$msg_pattern\\n" "$*" >&3; }
call() { printf "$call_pattern\\n" "${PS4:-+ }$*" >&3; "$@"; }
error() { printf "$err_pattern\\n" "Error: $*" >&3; exit 1; }
warning() { printf "$err_pattern\\n" "Warning: $*" >&3; }
exec 3>&2


call cd "${MAILCOW_DICKERIZED_DIR}"
call cp "${ROOTDIR}/mailcow/docker-compose.override.yml" "${MAILCOW_DICKERIZED_DIR}/docker-compose.override.yml"
# call docker compose pull
call docker compose up -d
