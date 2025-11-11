#!/usr/bin/env bash
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
#

#!/bin/bash
set -e

NETWORK_NAME="cluster-net"
SUBNET="172.18.0.0/16"
GATEWAY="172.18.0.1"
RUNTIME=${RUNTIME:-auto}  # choose: docker | podman | auto

create_network() {
  local cmd=$1
  local exists_cmd="$cmd network inspect $NETWORK_NAME >/dev/null 2>&1"

  if eval "$exists_cmd"; then
    echo "Network '$NETWORK_NAME' already exists in $cmd."
  else
    echo "Creating network '$NETWORK_NAME' in $cmd..."
    $cmd network create \
      --driver bridge \
      --subnet $SUBNET \
      --gateway $GATEWAY \
      $NETWORK_NAME
  fi
}

# Auto-detect or use user choice
if [ "$RUNTIME" = "docker" ]; then
  create_network docker
elif [ "$RUNTIME" = "podman" ]; then
  create_network podman
else
  if command -v docker >/dev/null 2>&1; then
    echo "Detected Docker (defaulting to Docker runtime)"
    create_network docker
  elif command -v podman >/dev/null 2>&1; then
    echo "Detected Podman (defaulting to Podman runtime)"
    create_network podman
  else
    echo "Neither Docker nor Podman found. Please install one."
    exit 1
  fi
fi
