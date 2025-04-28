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