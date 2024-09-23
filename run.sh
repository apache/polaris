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

# Initialize variables
BUILD_ARGS=""  # Initialize an empty string to store Docker build arguments

# Function to display usage information
usage() {
  echo "Usage: $0 [-b build-arg1=value1;build-arg2=value2;...] [-h]"
  echo "  -b    Pass a set of arbitrary build arguments to docker build, separated by semicolons"
  echo "  -h    Display this help message"
  exit 1
}

# Parse command-line arguments
while getopts "b:h" opt; do
  case ${opt} in
    b)
      IFS=';' read -ra ARGS <<< "${OPTARG}"  # Split the semicolon-separated list into an array
      for arg in "${ARGS[@]}"; do
        BUILD_ARGS+=" --build-arg ${arg}"  # Append each build argument to the list
      done
      ;;
    h)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

# Shift off the options and arguments
shift $((OPTIND-1))

# Deploy the registry
echo "Building Kind Registry..."
#sh ./kind-registry.sh

# Check if BUILD_ARGS is not empty and print the build arguments
if [[ -n "$BUILD_ARGS" ]]; then
  echo "Building polaris image with build arguments: $BUILD_ARGS"
else
  echo "Building polaris image without any additional build arguments."
fi

# Build and deploy the server image
echo "Building polaris image..."
docker build -t localhost:5001/polaris $BUILD_ARGS -f Dockerfile .
echo "Pushing polaris image..."
#docker push localhost:5001/polaris
echo "Loading polaris image to kind..."
#kind load docker-image localhost:5001/polaris:latest

echo "Applying kubernetes manifests..."
#kubectl delete -f k8/deployment.yaml --ignore-not-found
#kubectl apply -f k8/deployment.yaml
