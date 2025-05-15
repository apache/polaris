#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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

if [ "$COMPONENT" = "server" ]; then
    cd server
    java -jar quarkus-run.jar "$@"
else
    cd admin
    java -jar quarkus-run.jar "$@"
fi 