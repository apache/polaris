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

# Script to create the static website. Default is to create the static site for polaris.apache.org.
# The --local command line option also builds the static site to inspect it locally, it starts an
# http server for that purpose.

set -e

cd "$(dirname "$0")/../.."
. "site/bin/_hugo-docker-include.sh"

SERVICE=hugo_publish_apache

while [[ $# -gt 0 ]]; do
  case $1 in
    --local)
      SERVICE=hugo_publish_localhost8080
      shift
      ;;
    *)
      echo "Unknown option/argument $1"
      exit 1
      ;;
  esac
done

$COMPOSE \
  --project-name polaris_site \
  --file site/docker/docker-compose-publish.yml \
  $COMPOSE_ARGS \
  up \
  --build \
  --abort-on-container-exit \
  --exit-code-from $SERVICE \
  $SERVICE
