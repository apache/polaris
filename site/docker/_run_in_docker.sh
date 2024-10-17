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

# THIS SCRIPT IS ONLY INTENDED TO BE USED INSIDE THE HUGO DOCKER CONTAINER !

set -e

HUGO_ARGS=()
CREATE_SITE=0
RUN_HTTPD=0
DEST_DIR=""

while [[ $# -gt 0 ]]; do
  case $1 in
    create-site-publication)
      CREATE_SITE=1
      DEST_DIR="/polaris/site/build/apache-site"
      shift
      ;;
    create-local-site-publication)
      CREATE_SITE=1
      RUN_HTTPD=1
      DEST_DIR="/polaris/site/build/localhost-site"
      shift
      ;;
    *)
      HUGO_ARGS+=("$1")
      shift
      ;;
  esac
done

cd /polaris

if [[ ${CREATE_SITE} == 1 ]]; then
  npm install --save-dev autoprefixer postcss postcss-cli

  hugo \
    --source /polaris/site \
    --cacheDir /hugo/cache \
    --noBuildLock \
    --cleanDestinationDir \
    --printPathWarnings \
    --printMemoryUsage \
    --printI18nWarnings \
    --destination ${DEST_DIR} \
    --minify \
    "${HUGO_ARGS[@]}"

  if [[ ${RUN_HTTPD} == 1 ]]; then
    cd ${DEST_DIR}
    exec http-server -p 8080 --cors
  fi
else
  exec hugo serve \
    --source /polaris/site \
    --cacheDir /hugo/cache \
    --noBuildLock \
    --buildDrafts \
    --cleanDestinationDir \
    --printPathWarnings \
    --printMemoryUsage \
    --printI18nWarnings \
    --renderToMemory \
    --bind 0.0.0.0 \
    "${HUGO_ARGS[@]}"
fi
