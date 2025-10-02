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

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common logging functions and constants
source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"

function get_remote_url() {
  # Get the URL for a given remote, return non-zero if remote doesn't exist
  local remote_name="$1"
  git remote get-url "${remote_name}" 2>/dev/null || return 1
}

function check_github_checks_passed() {
  # Check that all GitHub checks have passed for a specific commit SHA.
  # More specifically, we check that all check runs have a conclusion of "success" or "skipped".
  # Returns 0 if all checks are "success" or "skipped", 1 otherwise
  local commit_sha="$1"

  print_info "Checking that all Github checks have passed for commit ${commit_sha}..."

  # Get repository information from the current Git repository
  local repo_info="$GITHUB_REPOSITORY"

  local num_invalid_checks
  local num_invalid_checks_retrieval_command="gh api repos/${repo_info}/commits/${commit_sha}/check-runs --jq '[.check_runs[] | select(.conclusion != \"success\" and .conclusion != \"skipped\" and (.name | startswith(\"Release - \") | not))] | length'"
  if [ ${DRY_RUN} -eq 1 ]; then
    print_info "DRY_RUN is enabled, skipping GitHub check verification"
    print_command "${num_invalid_checks_retrieval_command}"
    num_invalid_checks=0
  else
    # Ideally, we should be able to use num_invalid_checks=$(${num_invalid_checks_retrieval_command}) but it seems Github does not like that.  Use a temporary script instead.
    local temp_script=$(mktemp)
    echo "${num_invalid_checks_retrieval_command}" > "${temp_script}"
    num_invalid_checks=$(bash "${temp_script}")
    script_exit_code=$?
    rm -f "${temp_script}"
  fi

  if [[ $script_exit_code -ne 0 ]]; then
    print_error "Failed to fetch GitHub check runs for commit ${commit_sha}"
    return 1
  fi

  if [[ ${num_invalid_checks} -ne 0 ]]; then
    print_error "Found ${num_invalid_checks} failed or in-progress GitHub checks for commit ${commit_sha}"
    return 1
  fi

  print_info "All GitHub checks passed for commit ${commit_sha}"
  return 0
}