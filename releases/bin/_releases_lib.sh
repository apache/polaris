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
# Additional functionality for release related scripts that deal with
# Git tags/branches and versions inferred to/from those, based on `_lib.sh`.
#
# Includes worktree checks (non-dirty, upstream, etc).
#

if [[ -z ${bin_dir} ]]; then
  echo "bin_dir variable undefined, fix the issue in the calling script, aborting" > /dev/stderr
  exit 1
fi

. "${bin_dir}/_lib.sh"

branch_type=
version_major=
version_minor=
# shellcheck disable=SC2154
if [[ "${current_branch}" == "${main_branch}" ]]; then
  branch_type="main"
elif echo "${current_branch}" | grep --extended-regexp --quiet "${release_branch_regex}"; then
  # shellcheck disable=SC2034
  version_major="$(major_version_from_branch_name "${current_branch}")"
  version_minor="$(minor_version_from_branch_name "${current_branch}")"
  # shellcheck disable=SC2034
  [[ "x" == "${version_minor}" ]] && branch_type="major" || branch_type="minor"
else
  echo "Current branch '${current_branch}' must be either the main branch '${main_branch}' or a release branch following exactly the pattern '${release_branch_prefix}<MAJOR>.<MINOR>', aborting" > /dev/stderr
  exit 1
fi

if [[ -z ${upstream_name} ]]; then
  echo "Current branch '${current_branch}' has no remote, aborting" > /dev/stderr
  exit 1
fi

if [[ -n "$(git status --untracked-files=no --porcelain)" ]]; then
  echo "Current worktree has uncommitted changes, aborting" > /dev/stderr
  git status --untracked-files=no --porcelain > /dev/stderr
  exit 1
fi
