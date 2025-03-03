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
  echo "bin_dir variable undefined, fix the issue in the scripts, aborting" > /dev/stderr
  exit 1
fi

. "${bin_dir}/_lib.sh"

version_txt="$(cat ${worktree_dir}/version.txt)"
current_branch="$(git branch --show-current)"
upstream_name="$(git config branch."${current_branch}".remote)"

function list_release_branches {
  local prefix
  local suffix
  prefix="$1"
  suffix="$2"
  git ls-remote --branches "${upstream_name}" "${release_branch_prefix}${prefix}*${suffix}" | sed --regexp-extended 's/[0-9a-f]+\Wrefs\/heads\/(.*)/\1/'
  return
}

function list_release_tags {
  local prefix
  prefix="$1"
  git ls-remote --tags "${upstream_name}" "${tag_prefix}${prefix}*" | sed --regexp-extended 's/[0-9a-f]+\Wrefs\/tags\/(.*)/\1/'
  return
}

function major_version_from_branch_name {
  echo "$1" | sed --regexp-extended "s/${release_branch_regex}/\1/"
}

function minor_version_from_branch_name {
  echo "$1" | sed --regexp-extended "s/${release_branch_regex}/\2/"
}

function patch_version_from_tag {
  echo "$1" | sed --regexp-extended "s/${tag_regex}/\1/"
}

function rc_iteration_from_tag {
  echo "$1" | sed --regexp-extended "s/${tag_regex}/\4/"
}

function get_max_patch_version {
  local major
  local minor
  major="$1"
  minor="$2"
  max_patch=-1
  while read -r release_tag_name ; do
    _patch="$(patch_version_from_tag "${release_tag_name}")"
    [[ $_patch -gt $max_patch ]] && max_patch=$_patch
  done < <(list_release_tags "${major}.${minor}.")
  echo "${max_patch}"
}

function get_max_rc_iteration {
  local full_version
  full_version="$1"
  max_rc=-1
  while read -r release_tag_name ; do
    _rc="$(rc_iteration_from_tag "${release_tag_name}")"
    if [[ -z ${_rc} ]]; then
      max_rc="-2"
      break
    fi
    [[ $_rc -gt $max_rc ]] && max_rc=$_rc
  done < <(list_release_tags "${full_version}")
  echo "${max_rc}"
}

function get_max_major_version {
  max_major=-1
  while read -r release_branch_name ; do
    _major="$(major_version_from_branch_name "${release_branch_name}")"
    [[ $_major -gt $max_major ]] && max_major=$_major
  done < <(list_release_branches "" ".x")
  echo "${max_major}"
}

function get_max_minor_version {
  max_minor=-1
  while read -r release_branch_name ;do
    _minor="$(minor_version_from_branch_name "${release_branch_name}")"
    [[ $_minor -gt $max_minor ]] && max_minor=$_minor
  done < <(list_release_branches "${version_major}" "")
  echo "${max_minor}"
}

branch_type=
version_major=
version_minor=
if [[ "${current_branch}" == "${main_branch}" ]]; then
  branch_type="main"
elif echo "${current_branch}" | grep --extended-regexp --quiet "${release_branch_regex}"; then
  version_major="$(major_version_from_branch_name "${current_branch}")"
  version_minor="$(minor_version_from_branch_name "${current_branch}")"
  [[ "x" == "${version_minor}" ]] && branch_type="major" || branch_type="minor"
else
  echo "Current branch '${current_branch}' must be either the main branch '${main_branch}' or a release branch following exactly the pattern '${release_branch_prefix}<MAJOR>.<MINOR>', aborting" > /dev/stderr
  exit 1
fi

if [[ -z ${upstream_name} ]]; then
  echo "Current branch '${current_branch}' has no remote, aborting" > /dev/stderr
  exit 1
fi

if [[ ! -z "$(git status --untracked-files=no --porcelain)" ]]; then
  echo "Current worktree has uncommitted changes, aborting" > /dev/stderr
  git status --untracked-files=no --porcelain > /dev/stderr
  exit 1
fi
