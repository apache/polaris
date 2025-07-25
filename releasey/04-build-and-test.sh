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
# Build and Test Script
#
# Builds Polaris completely and ensures that all integration and regression tests
# have been run prior to releasing by verifying the status of all GitHub Actions
# workflows for the current commit.
#

set -euo pipefail

releases_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
libs_dir="${releases_dir}/libs"

source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_exec.sh"
source "${libs_dir}/_version.sh"
source "${libs_dir}/_github.sh"

function usage() {
  cat << EOF
$(basename "$0") [--help | -h]

  Builds Polaris completely and ensures that all integration and regression tests
  have been run prior to releasing by verifying GitHub CI status.

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

print_info "Starting build and test process..."
echo

# Clean and build Polaris
print_info "Building Polaris..."
exec_process ./gradlew clean build

current_commit=$(git rev-parse HEAD)
print_info "Current commit: ${current_commit}"

if ! check_github_checks_passed "${current_commit}"; then
  print_error "Integration and regression tests have not all passed for commit ${current_commit}"
  print_error "Please ensure all CI workflows are successful before proceeding with the release"
  print_info "You can check the status at: https://github.com/${GITHUB_REPO_OWNER}/${GITHUB_REPO_NAME}/commit/${current_commit}"
  exit 1
fi

echo
print_success "🎉 Build and test verification completed successfully!"