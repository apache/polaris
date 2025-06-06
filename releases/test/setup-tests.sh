#!/usr/bin/env bash
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

set -e

cd "${0%/*}"

function setup_bats_repo() {
  local repo="$1"
  local dir="$2"


  if [[ -d "${dir}" ]]; then
    (cd "$dir" ; git pull)
  else
    mkdir -p "${dir}"
    if [[ -n "${CI}" ]]; then
      git clone --depth 1 "${repo}" "${dir}"
    else
      git clone "${repo}" "${dir}"
    fi
  fi
}

setup_bats_repo https://github.com/bats-core/bats-core.git bats
setup_bats_repo https://github.com/bats-core/bats-support.git test_helper/bats-support
setup_bats_repo https://github.com/bats-core/bats-assert.git test_helper/bats-assert
setup_bats_repo https://github.com/bats-core/bats-file.git test_helper/bats-file

echo ""
echo ""
echo "==============================================================================="
echo "Run the following before running any tests!"
echo "   export PATH=$(realpath .)/bats/bin:\$PATH"
echo "==============================================================================="
echo ""
