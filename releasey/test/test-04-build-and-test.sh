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
# Test Script for 04-build-and-test.sh
#
# Tests the dry-run functionality of the build and test script by verifying the
# exact commands that would be executed.  It does not do much given that the
# script only run a single gradle command and interacts with Github to validate
# the state of the commit.
#

set -euo pipefail

test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
releases_dir="${test_dir}/.."
libs_dir="${releases_dir}/libs"

source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_version.sh"

function usage() {
  cat <<EOF
$(basename "$0") [--help | -h]

  Tests the 04-build-and-test.sh script by running it in dry-run mode
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

print_info "Starting test for 04-build-and-test.sh..."

print_info "Create temporary file to capture the commands that would be executed..."
temp_file=$(mktemp)
trap 'rm -f "$temp_file"' EXIT

print_info "Running script..."
# Set a dummy GitHub token for testing to avoid API calls
DRY_RUN=1 GITHUB_TOKEN="dummy_token" \
  "${releases_dir}/04-build-and-test.sh" \
  3>"$temp_file"

print_info "Verifying output content..."
# Read the actual content
actual_content=$(cat "$temp_file")

expected_content="./gradlew clean build
curl -s -H \"Authorization: Bearer dummy_token\" -H \"Accept: application/vnd.github+json\" -H \"X-GitHub-Api-Version: 2022-11-28\" https://api.github.com/repos/apache/polaris/commits/$(git rev-parse HEAD)/check-runs | jq -r '[.check_runs[].conclusion | select(. != \"success\" and . != \"skipped\")] | length'"

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
