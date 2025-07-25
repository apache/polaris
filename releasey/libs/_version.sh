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
# Utility functions for version validation and version.txt manipulation
#

libs_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$libs_dir/_constants.sh"
source "$libs_dir/_log.sh"

function validate_and_extract_rc_version {
  # This function validates the format of a release candidate version and extracts its components (major.minor.patch and rc number).
  # It returns 0 if the version is valid and sets the global variables major, minor, patch, and rc_number.
  # Otherwise, it returns 1.
  local version="$1"

  if [[ ! ${version} =~ ${VERSION_REGEX_RC} ]]; then
    return 1
  fi

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  rc_number="${BASH_REMATCH[4]}"

  return 0
}

function validate_and_extract_git_tag_version {
  # This function validates the format of a git tag version and extracts its components (major.minor.patch and rc number).
  # It is similar to validate_and_extract_rc_version, but for git tag format.
  # It returns 0 if the version is valid and sets the global variables major, minor, patch, and rc_number.
  # Otherwise, it returns 1.
  local version="$1"

  if [[ ! ${version} =~ ${VERSION_REGEX_GIT_TAG} ]]; then
    return 1
  fi

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  rc_number="${BASH_REMATCH[4]}"

  return 0
}

function update_version {
  local version="$1"
  # This function is only there for dry-run support.  Because of the
  # redirection, we cannot use exec_process with the exact command that will be
  # executed.
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    exec_process echo ${version} >$VERSION_FILE
  else
    exec_process "echo ${version} > $VERSION_FILE"
  fi
}

function ensure_cwd_is_project_root() {
  # This function verifies that the script is executed from the project root
  # directory and that the gradle wrapper script is present.
  if [[ ! -f "gradlew" ]]; then
    print_error "This script must be executed from the project root directory."
    print_error "Current directory: $(pwd)"
    return 1
  fi
}
