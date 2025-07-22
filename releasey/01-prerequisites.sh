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
# Release Prerequisites Verification Script
#
# This script ensures everything is ready for release:
# 1. GPG setup verification
# 2. Maven/Gradle credentials verification
# 3. Git remote setup verification
# 4. Docker setup verification (for Docker image releases)
#
# Returns non-zero exit code if any setup verification fails.
#

set -euo pipefail

releases_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
libs_dir="${releases_dir}/libs"

source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_exec.sh"
source "${libs_dir}/_files.sh"
source "${libs_dir}/_gpg.sh"
source "${libs_dir}/_nexus.sh"
source "${libs_dir}/_github.sh"
source "${libs_dir}/_docker.sh"

function usage() {
  cat << EOF
$(basename "$0") [--help | -h]

  Verifies that everything is ready for release:
  1. GPG setup verification
  2. Maven/Gradle credentials verification
  3. Git remote setup verification
  4. Docker setup verification (for Docker image releases)

  Options:
    -h --help
        Print usage information.

  Examples:
    $(basename "$0")

EOF
}

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

ensure_cwd_is_project_root

print_info "Starting release setup verification..."
echo

setup_failed=0

if ! ensure_gpg_setup_is_done; then
  print_error "GPG setup verification failed"
  setup_failed=1
fi
echo

if ! ensure_credentials_are_configured; then
  print_error "Maven/Gradle credentials verification failed"
  setup_failed=1
fi
echo

if ! ensure_github_setup_is_done; then
  print_error "Git remote setup verification failed"
  setup_failed=1
fi
echo

if ! ensure_docker_setup_is_done; then
  print_error "Docker setup verification failed"
  setup_failed=1
fi
echo

if [[ ${setup_failed} -eq 0 ]]; then
  print_success "🎉 All setup checks passed! Your environment is ready for releases."
  echo
  print_warning "Note that this script does not check write permissions to dist.apache.org."
  print_warning "Ensure you have those permissions before proceeding with releases."
  echo
  print_info "Next steps:"
  print_info "* Use the script 02-create-release-branch.sh to create a release branch."
  print_info "* Use the script 03-create-release-tag.sh to create a release tag."
else
  print_error "❌ One or more setup checks failed. Please fix the issues above before proceeding with releases."
  exit 1
fi