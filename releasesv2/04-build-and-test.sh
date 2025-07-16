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
# Builds Polaris completely and runs regression tests to verify the release.
# Cloud-specific tests are disabled by default and require proper credentials
# to be configured in the environment.
#

set -euo pipefail

releases_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
libs_dir="${releases_dir}/libs"

source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_exec.sh"
source "${libs_dir}/_files.sh"

function usage() {
  cat << EOF
$(basename "$0") [--help | -h]

  Builds Polaris completely and runs regression tests to verify the release.

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

print_info "Preparing regression test environment..."

# Clean the regtests output directory
print_info "Cleaning regression test output directory..."
exec_process rm -rf ./regtests/output
exec_process mkdir -p ./regtests/output
exec_process chmod -R 777 ./regtests/output

# Generate Python client
print_info "Regenerating Python client..."
exec_process ./gradlew regeneratePythonClient

# Build container image
print_info "Building Polaris container image..."
exec_process ./gradlew \
  :polaris-server:assemble \
  :polaris-server:quarkusAppPartsBuild --rerun \
  -Dquarkus.container-image.build=true

print_info "Running regression tests..."
print_warning "Cloud-specific tests are disabled"

# TODO: Enable cloud-specific tests when credentials are properly configured in GitHub Actions
# The following environment variables would need to be set to enable cloud tests:
# - AWS_TEST_ENABLED=true (plus AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_STORAGE_BUCKET, AWS_ROLE_ARN, AWS_TEST_BASE)
# - GCS_TEST_ENABLED=true (plus GCS_TEST_BASE, GOOGLE_APPLICATION_CREDENTIALS)
# - AZURE_TEST_ENABLED=true (plus AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_DFS_TEST_BASE, AZURE_BLOB_TEST_BASE)

# Run regression tests with cloud tests explicitly disabled
exec_process env \
  AWS_TEST_ENABLED=false \
  GCS_TEST_ENABLED=false \
  AZURE_TEST_ENABLED=false \
  AWS_CROSS_REGION_TEST_ENABLED=false \
  docker compose -f ./regtests/docker-compose.yml up --build --exit-code-from regtest

echo
print_success "🎉 Build and regression tests completed successfully!"
echo
print_info "All local regression tests passed. The release is ready for distribution."
print_info "Note: Cloud-specific tests were skipped. Enable them with proper credentials for full validation."
