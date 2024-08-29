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
ECLIPSELINK="false"  # Default value

# Function to display usage information
usage() {
  echo "Usage: $0 [-e true|false] [-h]"
  echo "  -e    Set the ECLIPSELINK flag (default: false)"
  echo "  -h    Display this help message"
  exit 1
}

# Parse command-line arguments
while getopts "e:h" opt; do
  case ${opt} in
    e)
      ECLIPSELINK=${OPTARG}
      if [[ "$ECLIPSELINK" != "true" && "$ECLIPSELINK" != "false" ]]; then
        echo "Error: Invalid value for -e. Use 'true' or 'false'."
        usage
      fi
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
sh ./kind-registry.sh

# Build and deploy the server image
echo "Building polaris image with ECLIPSELINK=$ECLIPSELINK..."
docker build -t localhost:5001/polaris --build-arg ECLIPSELINK=$ECLIPSELINK -f Dockerfile .
echo "Pushing polaris image..."
docker push localhost:5001/polaris
echo "Loading polaris image to kind..."
kind load docker-image localhost:5001/polaris:latest

echo "Applying Kubernetes manifests..."
kubectl delete -f k8/deployment.yaml --ignore-not-found
kubectl apply -f k8/deployment.yaml
