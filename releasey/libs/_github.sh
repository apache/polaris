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
# Git Remote Setup Script
#
# This script ensures that the Git repository has the proper remote configuration for Apache Polaris releases:
# 1. Checks whether there is a Git remote named 'apache' pointing to https://github.com/apache/polaris.git
# 2. If the remote doesn't exist, it will be added
# 3. If the remote exists but points to the wrong URL, error out
#
# Returns non-zero exit code if any verification fails.
#

set -euo pipefail

libs_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common logging functions and constants
source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_exec.sh"

function check_remote_exists() {
  # Check if a remote exists, return 0 if it exists, 1 if not
  local remote_name="$1"
  git remote | grep -q "^${remote_name}$"
}

function get_remote_url() {
  # Get the URL for a given remote, return non-zero if remote doesn't exist
  local remote_name="$1"
  git remote get-url "${remote_name}" 2>/dev/null || return 1
}

function ensure_no_uncommitted_change() {
  if [[ -n $(git status --porcelain) ]]; then
    print_error "There are uncommitted changes in the repository."
    print_error "A release can only be performed from a clean state."
    return 1
  fi
}

function add_apache_remote() {
  # Add the apache remote to the current repository, return non-zero if it fails
  print_info "Adding remote '${APACHE_REMOTE_NAME}' with URL '${APACHE_REMOTE_URL}'..."
  if exec_process git remote add "${APACHE_REMOTE_NAME}" "${APACHE_REMOTE_URL}"; then
    print_success "Successfully added remote '${APACHE_REMOTE_NAME}'"
    return 0
  else
    print_error "Failed to add remote '${APACHE_REMOTE_NAME}'"
    return 1
  fi
}

function ensure_github_setup_is_done() {
  print_info "Checking Git remote configuration for '${APACHE_REMOTE_NAME}'..."

  if ! check_remote_exists "${APACHE_REMOTE_NAME}"; then
    print_info "Remote '${APACHE_REMOTE_NAME}' does not exist, creating it..."
    if ! add_apache_remote; then
      return 1
    fi
  else
    print_info "Remote '${APACHE_REMOTE_NAME}' already exists, checking URL..."
    local current_url
    current_url=$(get_remote_url "${APACHE_REMOTE_NAME}")
    if [[ "${current_url}" != "${APACHE_REMOTE_URL}" ]]; then
      print_error "Remote '${APACHE_REMOTE_NAME}' exists but points to wrong URL: ${current_url}"
      return 1
    fi
  fi

  print_success "All Git remote setup checks passed! Your Git repository is properly configured for releases."
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  ensure_github_setup_is_done "$@"
fi
