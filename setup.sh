#!/bin/bash

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
