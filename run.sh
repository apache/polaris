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
#

# Runs Polaris as a mini-deployment locally. Creates two pods that bind themselves to port 8181.

# Function to display usage information
usage() {
  echo "Usage: $0 [--eclipse-link-deps=<deps>] [-h|--help]"
  echo "  --eclipse-link-deps=<deps>  EclipseLink dependencies to use, e.g."
  echo "                              --eclipse-link-deps=com.h2database:h2:2.3.232"
  echo "  -h, --help                  Display this help message"
  exit 1
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
  case $1 in
    --eclipse-link-deps=*)
      ECLIPSE_LINK_DEPS="-PeclipseLinkDeps=${1#*=}"
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
  shift
done

# Deploy the registry
echo "Building Kind Registry..."
sh ./kind-registry.sh

# Build and deploy the server image
echo "Building polaris image..."
./gradlew \
  :polaris-quarkus-server:build \
  :polaris-quarkus-server:quarkusAppPartsBuild --rerun \
  $ECLIPSE_LINK_DEPS \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.registry=localhost:5001

echo "Pushing polaris image..."
docker push localhost:5001/apache/polaris

echo "Loading polaris image to kind..."
kind load docker-image localhost:5001/apache/polaris:latest

echo "Applying kubernetes manifests..."
kubectl delete -f k8/deployment.yaml --ignore-not-found
kubectl apply -f k8/deployment.yaml
