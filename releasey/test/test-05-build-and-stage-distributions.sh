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
# Test Script for 05-build-and-stage-distributions.sh
#
# Tests the dry-run functionality of the build and stage distributions script
# by verifying the exact commands that would be executed.
#

set -euo pipefail

test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
releasey_dir=$(cd "${test_dir}/.." && pwd)
LIBS_DIR="${releasey_dir}/libs"

source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_version.sh"

function usage() {
  cat <<EOF
$(basename "$0") <tag> [--help | -h]

  Tests the 05-build-and-stage-distributions.sh script by running it in dry-run mode
  and verifying the commands that would be executed.  This script creates a tag for
  testing purposes: apache-polaris-99.98.97-incubating-rc96.  The tag is deleted after
  the test is complete.

  Options:
    -h --help
        Print usage information.

  Examples:
    $(basename "$0")

EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
  --help | -h)
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

tag="apache-polaris-99.98.97-incubating-rc96"
version="99.98.97-incubating"
rc_number=96

print_info "Starting test for 05-build-and-stage-distributions.sh..."
print_info "Tag: ${tag}"
print_info "Version: ${version}"

# Create a temporary tag for testing purposes
print_info "Creating temporary tag: ${tag}..."
git tag "${tag}"
trap 'git tag -d "${tag}"' EXIT

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

print_info "Create temporary file to capture the commands that would be executed..."
temp_file=$(mktemp)

# Ensure we always clean up
trap 'rm -f "$temp_file"; git checkout "$restore_target" >/dev/null 2>&1; git tag -d "${tag}"' EXIT

print_info "Running script..."
DRY_RUN=1 \
  "${releasey_dir}/05-build-and-stage-distributions.sh" \
  3>"$temp_file"

print_info "Verifying output content..."
actual_content=$(cat "$temp_file")

expected_content="cd ${releasey_dir}/..
./gradlew build sourceTarball -Prelease -PuseGpgAgent -x test -x intTest
cd ${releasey_dir}/../helm
helm package polaris
helm gpg sign polaris-${version}.tgz
shasum -a 512 polaris-${version}.tgz > polaris-${version}.tgz.sha512
gpg --armor --output polaris-${version}.tgz.asc --detach-sig polaris-${version}.tgz
shasum -a 512 polaris-${version}.tgz.prov > polaris-${version}.tgz.prov.sha512
gpg --armor --output polaris-${version}.tgz.prov.asc --detach-sig polaris-${version}.tgz.prov
svn co https://dist.apache.org/repos/dist/dev/incubator/polaris ${releasey_dir}/polaris-dist-dev
mkdir -p ${releasey_dir}/polaris-dist-dev/${version}
mkdir -p ${releasey_dir}/polaris-dist-dev/helm-chart/${version}
cp build/distribution/* ${releasey_dir}/polaris-dist-dev/${version}/
cp runtime/distribution/build/distributions/* ${releasey_dir}/polaris-dist-dev/${version}/
cp helm/polaris-${version}.tgz* ${releasey_dir}/polaris-dist-dev/helm-chart/${version}/
svn add ${releasey_dir}/polaris-dist-dev/${version}
svn add ${releasey_dir}/polaris-dist-dev/helm-chart/${version}
svn commit -m Stage Apache Polaris ${version} RC${rc_number}
cd ${releasey_dir}/polaris-dist-dev/helm-chart
helm repo index .
cd ${releasey_dir}/..
./gradlew publishToApache -Prelease -PuseGpgAgent -Dorg.gradle.parallel=false"

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
