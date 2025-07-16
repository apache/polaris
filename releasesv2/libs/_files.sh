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
# Utility function for version.txt manipulation
#

libs_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$libs_dir/_constants.sh"
source "$libs_dir/_log.sh"

function update_version {
  # This function is only there for dry-run support.  Because of the
  # redirection, we cannot use exec_process with the exact command that will be
  # executed.
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    exec_process echo ${polaris_version} >$VERSION_FILE
  else
    exec_process "echo ${polaris_version} > $VERSION_FILE"
  fi
}

function ensure_cwd_is_project_root() {
  # This function verifies that the script is executed from the project root
  # directory and that the gradle wrapper script is present.
  if [[ ! -f "gradlew" ]] || [[ ! -d ".git" ]]; then
    print_error "This script must be executed from the project root directory."
    print_error "Current directory: $(pwd)"
    return 1
  fi
}
