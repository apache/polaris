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
# Test Script for 02-create-release-branch.sh
#
# Tests the dry-run functionality of the create release branch script
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
$(basename "$0") [--help | -h]

  Tests the 02-create-release-branch.sh script by running it in dry-run mode
  and verifying the commands that would be executed.

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

print_info "Starting test for 02-create-release-branch.sh..."

print_info "Create temporary file to capture the commands that would be executed..."
temp_file=$(mktemp)
trap 'rm -f "$temp_file"' EXIT

print_info "Running script..."
version_before_script=$(cat "${VERSION_FILE}")
DRY_RUN=1 \
  "${releasey_dir}/02-create-release-branch.sh" \
  --version 42.41.40-incubating-rc1 \
  3>"$temp_file"

print_info "Verifying output content..."
# Read the actual content
actual_content=$(cat "$temp_file")

# Define expected content
expected_content="git checkout HEAD
git branch release/42.41.40-incubating
git push apache release/42.41.40-incubating --set-upstream
git checkout release/42.41.40-incubating
echo 42.41.40-incubating > ${LIBS_DIR}/../../version.txt
sed -i~ s/${version_before_script}/42.41.40-incubating/g ${LIBS_DIR}/../../helm/polaris/Chart.yaml
sed -i~ s/${version_before_script}/42.41.40-incubating/g ${LIBS_DIR}/../../helm/polaris/values.yaml
sed -i~ s/${version_before_script}/42.41.40-incubating/g ${LIBS_DIR}/../../helm/polaris/README.md
sed -i~ s/${version_before_script//-/--}/42.41.40--incubating/ ${LIBS_DIR}/../../helm/polaris/README.md
git add ${LIBS_DIR}/../../version.txt ${LIBS_DIR}/../../helm/polaris/Chart.yaml ${LIBS_DIR}/../../helm/polaris/README.md ${LIBS_DIR}/../../helm/polaris/values.yaml
git commit -m [chore] Bump version to 42.41.40-incubating for release
git push release/42.41.40-incubating
./gradlew patchChangelog
git add ${LIBS_DIR}/../../CHANGELOG.md
git commit -m [chore] Update changelog for release
git push release/42.41.40-incubating"

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
