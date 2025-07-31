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

#
# Build and Stage Docker Images Script
#
# Builds and publishes multi-platform Docker images to DockerHub for release candidates.
# This script automates the "Build and staging Docker images" step from the release guide.
#

set -euo pipefail

releasey_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIBS_DIR="${releasey_dir}/libs"

source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"
source "${LIBS_DIR}/_version.sh"

function usage() {
  cat << EOF
$(basename "$0") [--help | -h]

  Builds and publishes multi-platform Docker images to DockerHub for release candidates.
  The version is automatically determined from the current git tag. I.e. this script must
  be run from a release candidate tag.

  Prerequisites:
    - Docker with buildx support must be installed and configured
    - DockerHub credentials must be configured (docker login)
    - Must be run from a release candidate tag (apache-polaris-x.y.z-incubating-rcN)

  Options:
    -h --help
        Print usage information.

  Examples:
    $(basename "$0")

EOF
}

ensure_cwd_is_project_root

while [[ $# -gt 0 ]]; do
  case $1 in
    --help|-h)
      usage
      exit 0
      ;;
    *)
      print_error "Unknown option/argument $1"
      usage >&2
      exit 1
      ;;
  esac
done

# Determine version from current git tag
print_info "Determining version from current git tag..."

if ! git_tag=$(git describe --tags --exact-match HEAD 2>/dev/null); then
  print_error "Current HEAD is not on a release candidate tag. Please checkout a release candidate tag first."
  print_error "Use: git checkout apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi
print_info "Found git tag: ${git_tag}"

# Validate git tag format and extract version components
if ! validate_and_extract_git_tag_version "${git_tag}"; then
  print_error "Invalid git tag format: ${git_tag}"
  print_error "Expected format: apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi

version="${major}.${minor}.${patch}-incubating"
docker_tag="${version}-rc${rc_number}"

print_info "Starting Docker image build and staging process..."
print_info "Version: ${version}"
print_info "RC number: ${rc_number}"
print_info "Docker tag: ${docker_tag}"
echo

# Build and push polaris-server Docker image
print_info "Building and pushing polaris-server Docker image..."
exec_process ./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=true \
  -Dquarkus.docker.buildx.platform="linux/amd64,linux/arm64" \
  -Dquarkus.container-image.tag="${docker_tag}"

# Build and push polaris-admin Docker image
print_info "Building and pushing polaris-admin Docker image..."
exec_process ./gradlew :polaris-admin:assemble :polaris-admin:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true \
  -Dquarkus.container-image.push=true \
  -Dquarkus.docker.buildx.platform="linux/amd64,linux/arm64" \
  -Dquarkus.container-image.tag="${docker_tag}"

echo
print_success "🎉 Docker images built and staged successfully!"
echo
print_info "Published Docker images:"
print_info "- polaris-server: ${DOCKER_REGISTRY}/apache/polaris:${docker_tag}"
print_info "- polaris-admin: ${DOCKER_REGISTRY}/apache/polaris-admin-tool:${docker_tag}"
echo
print_info "Docker Hub links:"
print_info "- ${DOCKER_HUB_URL}/r/apache/polaris/tags"
print_info "- ${DOCKER_HUB_URL}/r/apache/polaris-admin-tool/tags"
echo
print_info "Next steps:"
print_info "1. Start the vote thread"
