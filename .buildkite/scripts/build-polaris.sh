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
# Placeholder script to build and validate Fivetran's fork of Polaris
# This script is called from the main pipeline job
# Add commands below as needed
export IMAGE_TAG=${BUILDKITE_COMMIT:0:7}
buildkite-agent meta-data set "image-tag" "$IMAGE_TAG"

docker buildx create --name multiarch --driver=docker-container --platform linux/amd64,linux/arm64 --bootstrap --use

if [ -z "$REPOSITORY" ]; then
    REPOSITORY=$(echo "$REPOSITORY" | sed 's://:/:g')
    echo "Skipping image upload: Repository variable is empty."
    docker buildx build --load --platform linux/amd64 -t polaris:${IMAGE_TAG} .
else
    echo "Building image."
    docker buildx build --load --platform linux/amd64 -t ${REPOSITORY}/polaris:${IMAGE_TAG} .
    docker push ${REPOSITORY}/polaris:${IMAGE_TAG}
fi