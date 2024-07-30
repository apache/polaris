#!/bin/bash

#
# Copyright (c) 2024 Snowflake Computing Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CURRENT_DIR=$(pwd)

# deploy the registry
echo "Building Kind Registry..."
sh ./kind-registry.sh

# build and deploy the server image
echo "Building polaris image..."
docker build -t localhost:5001/polaris -f Dockerfile .
echo "Pushing polaris image..."
docker push localhost:5001/polaris
echo "Loading polaris image to kind..."
kind load docker-image localhost:5001/polaris:latest

echo "Applying kubernetes manifests..."
kubectl delete -f k8/deployment.yaml --ignore-not-found
kubectl apply -f k8/deployment.yaml
