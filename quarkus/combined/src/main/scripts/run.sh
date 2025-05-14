#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Default to server if no component specified
COMPONENT=${1:-server}

if [ "$COMPONENT" != "server" ] && [ "$COMPONENT" != "admin" ]; then
    echo "Usage: $0 [server|admin] [additional arguments...]"
    exit 1
fi

# Shift off the first argument so $@ contains remaining args
shift

# Common JVM options
JAVA_OPTS="${JAVA_OPTS} -Dquarkus.http.host=0.0.0.0"

if [ "$COMPONENT" = "server" ]; then
    echo "Starting Polaris Server..."
    cd server
    java ${JAVA_OPTS} -jar quarkus-run.jar "$@"
else
    echo "Starting Polaris Admin Tool..."
    cd admin
    java ${JAVA_OPTS} -jar quarkus-run.jar "$@"
fi 