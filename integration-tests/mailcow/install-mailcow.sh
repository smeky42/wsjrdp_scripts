#!/usr/bin/env bash

set -euo pipefail
# shopt -s nullglob

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


cd "$ROOTDIR"
call pwd
call rm -rf .mailcow-dockerized
call git clone https://github.com/mailcow/mailcow-dockerized "${MAILCOW_DICKERIZED_DIR}"
call cd "${MAILCOW_DICKERIZED_DIR}"
call pwd

mkdir bin
export PATH="$(pwd)/bin:$PATH"
if [ "$(uname -s)" = Darwin ]; then
    call ln -s $(which gcp) bin/cp
fi


export SKIP_CLAMD=y
export MAILCOW_HOSTNAME=worldscoutjamboree.local
export MAILCOW_BRANCH="master"
export MAILCOW_DBPASS=moohoo
export MAILCOW_DBROOT=moohoo
export MAILCOW_REDISPASS=moohoo
export MAILCOW_TZ=Europe/Berlin
call ./generate_config.sh --dev

call mv mailcow.conf mailcow.conf.original
sed -E \
    -e 's/HTTP_PORT=.*/HTTP_PORT=5080/' \
    -e 's/HTTP_BIND=.*/HTTP_BIND=127.0.0.1/' \
    -e 's/HTTPS_PORT=.*/HTTPS_PORT=5443/' \
    -e 's/HTTPS_BIND=.*/HTTPS_BIND=127.0.0.1/' \
    -e 's/HTTP_REDIRECT=.*/HTTP_REDIRECT=n/' \
    -e 's/COMPOSE_PROJECT_NAME=.*/COMPOSE_PROJECT_NAME=mailcow-dev/' \
    -e 's/SKIP_LETS_ENCRYPT=.*/SKIP_LETS_ENCRYPT=y/' \
    -e 's/SKIP_OLEFY=.*/SKIP_OLEFY=y/' \
    -e 's/SKIP_FTS=.*/SKIP_FTS=y/' \
    -e 's/ALLOW_ADMIN_EMAIL_LOGIN=.*/ALLOW_ADMIN_EMAIL_LOGIN=y/' \
    -e 's/USE_WATCHDOG=.*/USE_WATCHDOG=n/' \
    -e 's/#?API_KEY=.*/API_KEY=mailcow123/' \
    -e 's/#?API_ALLOW_FROM=.*/API_ALLOW_FROM=172.22.1.1,127.0.0.1,host.docker.internal/' \
    -e 's/DISABLE_NETFILTER_ISOLATION_RULE=.*/DISABLE_NETFILTER_ISOLATION_RULE=y/' \
    -e 's/SKIP_UNBOUND_HEALTHCHECK=.*/SKIP_UNBOUND_HEALTHCHECK=y/' \
    mailcow.conf.original \
    > mailcow.conf
call chmod 0600 mailcow.conf
call ls -l
call cp "${ROOTDIR}/mailcow/docker-compose.override.yml" "${MAILCOW_DICKERIZED_DIR}/docker-compose.override.yml"

call docker compose pull
