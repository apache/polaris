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

source "$LIBS_DIR/_constants.sh"
source "$LIBS_DIR/_log.sh"

function exec_process {
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    print_command "Executing '${*}'"
    "$@"
  else
    print_command "Dry-run, WOULD execute '${*}'"
  fi
}

# Executes a command with retry logic
# Args:
#   $1: max_attempts - Maximum number of retry attempts
#   $2: sleep_duration - Seconds to wait between retries
#   $3: cleanup_path - Path to clean up before retrying (can be empty)
#   $@: Command and arguments to execute
function exec_process_with_retries {
  if [[ $# -lt 4 ]]; then
    echo "ERROR: exec_process_with_retries requires: max_attempts sleep_duration cleanup_path command [args...]"
    exit 1
  fi

  local max_attempts="${1}"
  local sleep_duration="${2}"
  local cleanup_path="${3}"
  shift 3

  local attempt=1
  while true; do
    if exec_process "$@"; then
      break
    fi
    if [[ $attempt -ge $max_attempts ]]; then
      echo "ERROR: Command failed after ${max_attempts} attempts: ${*}"
      exit 1
    fi
    echo "WARNING: Command failed (attempt ${attempt}/${max_attempts}), retrying in ${sleep_duration} seconds..."
    # Clean up any partial state before retrying
    if [[ -n "${cleanup_path}" && -e "${cleanup_path}" ]]; then
      rm -rf "${cleanup_path}"
    fi
    sleep "${sleep_duration}"
    ((attempt++))
  done
}

function calculate_sha512 {
  local source_file="$1"
  local target_file="${source_file}.sha512"
  # This function is only there for dry-run support.  Because of the
  # redirection, we cannot use exec_process with the exact command that will be
  # executed.
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    exec_process shasum -a 512 "${source_file}" > "${target_file}"
  else
    exec_process "shasum -a 512 ${source_file} > ${target_file}"
  fi
}
