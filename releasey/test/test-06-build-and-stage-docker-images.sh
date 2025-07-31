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
# Test Script for 06-build-and-stage-docker-images.sh
#
# Tests the dry-run functionality of the Docker image build and staging script
# by verifying the exact commands that would be executed.
#

set -euo pipefail

test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
releasey_dir="${test_dir}/.."
LIBS_DIR="${releasey_dir}/libs"

source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_version.sh"

function usage() {
  cat <<EOF
$(basename "$0") <tag> [--help | -h]

  Tests the 06-build-and-stage-docker-images.sh script by running it in dry-run mode
  and verifying the commands that would be executed.

  Arguments:
    tag
        The git tag to checkout before running the test (e.g., apache-polaris-1.1.0-incubating-rc1)

  Options:
    -h --help
        Print usage information.

  Examples:
    $(basename "$0") apache-polaris-1.1.0-incubating-rc1

EOF
}

ensure_cwd_is_project_root

# Parse arguments
tag=""

while [[ $# -gt 0 ]]; do
  case $1 in
  --help | -h)
    usage
    exit 0
    ;;
  *)
    if [[ -z "$tag" ]]; then
      tag="$1"
    else
      print_error "Unknown option/argument $1"
      usage >&2
      exit 1
    fi
    ;;
  esac
  shift
done

# Validate required arguments
if [[ -z "$tag" ]]; then
  print_error "Tag parameter is required"
  usage >&2
  exit 1
fi

# Validate tag format and extract version components
if ! validate_and_extract_git_tag_version "${tag}"; then
  print_error "Invalid tag format: ${tag}"
  print_error "Expected format: apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi

version="${major}.${minor}.${patch}-incubating"
docker_tag="${version}-rc${rc_number}"

print_info "Starting test for 06-build-and-stage-docker-images.sh..."
print_info "Tag: ${tag}"
print_info "Version: ${version}"
print_info "Docker tag: ${docker_tag}"

# Save current git branch or ref
print_info "Saving current git state..."
if current_branch=$(git symbolic-ref --short HEAD 2>/dev/null); then
  print_info "Current branch: ${current_branch}"
  restore_target="${current_branch}"
else
  current_ref=$(git rev-parse HEAD)
  print_info "Current ref (detached HEAD): ${current_ref}"
  restore_target="${current_ref}"
fi

# Checkout the specified tag
print_info "Checking out tag: ${tag}..."
if ! git checkout "${tag}" >/dev/null 2>&1; then
  print_error "Failed to checkout tag: ${tag}"
  exit 1
fi
trap 'rm -f "$temp_file"; git checkout "$restore_target" >/dev/null 2>&1' EXIT

print_info "Create temporary file to capture the commands that would be executed..."
temp_file=$(mktemp)

print_info "Running script (version determined from current git tag)..."
DRY_RUN=1 \
  "${releasey_dir}/06-build-and-stage-docker-images.sh" \
  3>"$temp_file"

# Restore original git state
print_info "Restoring original git state: ${restore_target}..."
if ! git checkout "${restore_target}" >/dev/null 2>&1; then
  print_error "Failed to restore original git state: ${restore_target}"
  exit 1
fi

print_info "Verifying output content..."
actual_content=$(cat "$temp_file")

# Generate expected content based on the tag
expected_content="./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild --rerun -Dquarkus.container-image.build=true -Dquarkus.container-image.push=true -Dquarkus.docker.buildx.platform=linux/amd64,linux/arm64 -Dquarkus.container-image.tag=${docker_tag}
./gradlew :polaris-admin:assemble :polaris-admin:quarkusAppPartsBuild --rerun -Dquarkus.container-image.build=true -Dquarkus.container-image.push=true -Dquarkus.docker.buildx.platform=linux/amd64,linux/arm64 -Dquarkus.container-image.tag=${docker_tag}"

# Compare content
if [[ "$actual_content" == "$expected_content" ]]; then
  print_success "🎉 Test passed! Output content matches expected result."
else
  print_error "❌ Test failed! Output content does not match expected result."
  echo
  diff -u <(echo "$expected_content") <(echo "$actual_content")
  echo
  print_error "Content verification failed"
  exit 1
fi
