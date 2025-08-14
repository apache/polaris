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

# Create a new release version branch
#
# If called on the main branch, creates the release branch for the next major version.
# If called on a version branch, the tool errors out.

set -e
bin_dir="$(dirname "$0")"
. "${bin_dir}/_releases_lib.sh"

# shellcheck disable=SC2154
echo "Release Git remote name '${upstream_name}'"
echo ""
echo "List of release branches:"

list_release_branches "" | while read -r release_branch_name ;do
  _major="$(major_version_from_branch_name "${release_branch_name}")"
  _minor="$(minor_version_from_branch_name "${release_branch_name}")"
  echo "Release branch for ${_major}.${_minor} is ${release_branch_name}"
done
