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
# Constants and generic functions for all release related scripts
#

if [[ -z ${bin_dir} ]]; then
  echo "bin_dir variable undefined, fix the issue in the scripts, aborting" > /dev/stderr
  exit 1
fi

root_dir="$(realpath "${bin_dir}/../..")"
# shellcheck disable=SC2034
worktree_dir="${root_dir}/build/releases-git-worktree"
if [[ ! -f ${root_dir}/version.txt ]]; then
  echo "Looks like ${root_dir}/version.txt does not exist, aborting" > /dev/stderr
  exit 1
fi

# Constants
# shellcheck disable=SC2034
main_branch="releases-infra"
# shellcheck disable=SC2034
release_branch_prefix="release/"
# shellcheck disable=SC2034
release_branch_regex="^release\\/([0-9]+)[.](x|[0-9]+)([.](x|[0-9]+))?$"
# shellcheck disable=SC2034
versioned_docs_branch="versioned-docs"
tag_prefix="apache-polaris-"
# shellcheck disable=SC2034
rc_tag_regex="${tag_prefix}[0-9]+[.][0-9]+[.]([0-9]+)(-.+)*-rc([0-9]+)"
# shellcheck disable=SC2034
release_tag_regex="${tag_prefix}([0-9]+[.][0-9]+[.][0-9]+-.+)*"
# When going becoming a TLP, replace 'incubator/polaris' with 'polaris' !
project_release_dir_part="incubator/polaris"
# shellcheck disable=SC2034
svn_dist_dev_repo="https://dist.apache.org/repos/dist/dev/${project_release_dir_part}/"
# shellcheck disable=SC2034
svn_dist_release_repo="https://dist.apache.org/repos/dist/release/${project_release_dir_part}/"

# common subdirectory for local SVN directories, always set from tests
[[ -z ${svn_dir_prefix} ]] && svn_dir_prefix="build"
# shellcheck disable=SC2034
svn_dir_dev="${svn_dir_prefix}/svn-source-dev"
# shellcheck disable=SC2034
svn_dir_release="${svn_dir_prefix}/svn-source-release"



function get_podling_version_suffix {
  local podling
  podling="$(curl https://whimsy.apache.org/public/public_ldap_projects.json 2>/dev/null | jq --raw-output '.projects["polaris"]."podling"')"
  if [[ ${podling} == "current" ]] ; then
    echo "-incubating"
  fi
}

function start_group {
  local heading
  heading="${*}"
  if [[ ${CI} ]] ; then
    # For GitHub workflows
    echo "::group::${heading}"
  else
    # For local runs
    echo ""
    echo "${heading}"
    echo "-------------------------------------------------------------"
    echo ""
  fi
}

function end_group {
  if [[ ${CI} ]] ; then
    # For GitHub workflows
    echo "::endgroup::"
  else
    # For local runs
    echo ""
    echo "-------------------------------------------------------------"
    echo ""
  fi
}

function exec_process {
  local dry_run
  dry_run=$1
  shift

  if [[ ${dry_run} -ne 1 ]]; then
    echo "Executing '${*}'"
    "$@"
    return
  else
    echo "Dry-run, WOULD execute '${*}'"
  fi
}

# shellcheck disable=SC2034
# shellcheck disable=SC2154
version_txt="$(cat "${root_dir}"/version.txt)"
# shellcheck disable=SC2034
current_branch="$(git branch --show-current)"
# The name of the Git remote that points to the Apache Polaris repo
upstream_name="$(git remote -v | grep 'https://github.com/apache/polaris.git' | grep -w '(push)' | cut -f1)"

function list_release_branches {
  local prefix
  prefix="$1"
  # shellcheck disable=SC2154
  git ls-remote --branches "${upstream_name}" "${release_branch_prefix}${prefix}*" | sed --regexp-extended 's/[0-9a-f]+\Wrefs\/heads\/(.*)/\1/'
  return
}

function list_release_tags {
  local prefix
  prefix="$1"
  # shellcheck disable=SC2154
  git ls-remote --refs --tags "${upstream_name}" "${tag_prefix}${prefix}*" | sed --regexp-extended 's/[0-9a-f]+\Wrefs\/tags\/(.*)/\1/'
  return
}

function major_version_from_branch_name {
  local r
  # shellcheck disable=SC2154
  r="$(echo "$1" | sed --regexp-extended "s/${release_branch_regex}/\1/")"
  [[ "$r" != "$1" ]] && echo "$r"
}

function minor_version_from_branch_name {
  local r
  r="$(echo "$1" | sed --regexp-extended "s/${release_branch_regex}/\2/")"
  [[ "$r" != "$1" ]] && echo "$r"
}

function full_version_with_label_from_release_tag {
  local r
  r="$(echo "$1" | sed --regexp-extended "s/${release_tag_regex}/\1/")"
  [[ "$r" != "$1" ]] && echo "$r"
}

function patch_version_from_rc_tag {
  local r
  # shellcheck disable=SC2154
  r="$(echo "$1" | sed --regexp-extended "s/${rc_tag_regex}/\1/")"
  [[ "$r" != "$1" ]] && echo "$r"
}

function version_label_from_rc_tag {
  local r
  r="$(echo "$1" | sed --regexp-extended "s/${rc_tag_regex}/\2/")"
  [[ "$r" != "$1" ]] && echo "$r"
}

function rc_iteration_from_tag {
  local r
  r="$(echo "$1" | sed --regexp-extended "s/${rc_tag_regex}/\3/")"
  [[ "$r" != "$1" ]] && echo "$r"
}

function _get_max_patch_version {
  max_patch=-1
  while read -r release_tag_name ; do
    _patch="$(patch_version_from_rc_tag "${release_tag_name}")"
    [[ $_patch -gt $max_patch ]] && max_patch=$_patch
  done
  echo "${max_patch}"
}

function get_max_patch_version {
  local major
  local minor
  major="$1"
  minor="$2"
  _get_max_patch_version < <(list_release_tags "${major}.${minor}.")
}

function _get_max_rc_iteration {
  max_rc=-1
  while read -r release_tag_name ; do
    _rc="$(rc_iteration_from_tag "${release_tag_name}")"
    if [[ -z ${_rc} ]]; then
      max_rc="-2"
      break
    fi
    [[ $_rc -gt $max_rc ]] && max_rc=$_rc
  done
  echo "${max_rc}"
}

function get_max_rc_iteration {
  local full_version
  full_version="$1"
  _get_max_rc_iteration < <(list_release_tags "${full_version}")
}

function _tag_for_full_version {
  while read -r release_tag_name ; do
    _rc="$(rc_iteration_from_tag "${release_tag_name}")"
    if [[ -z ${_rc} ]]; then
      echo "${release_tag_name}"
      break
    fi
  done
}

function tag_for_full_version {
  local full_version
  full_version="$1"
  _tag_for_full_version < <(list_release_tags "${full_version}")
}

function _get_latest_rc_tag_name {
  max_rc=-1
  tag=""
  while read -r release_tag_name ; do
    _rc="$(rc_iteration_from_tag "${release_tag_name}")"
    if [[ -z ${_rc} ]]; then
      max_rc="-2"
      break
    fi
    if [[ $_rc -gt $max_rc ]]; then
      max_rc=$_rc
      tag="${release_tag_name}"
    fi
  done
  echo "${tag}"
}

function get_latest_rc_tag_name {
  local full_version
  full_version="$1"
  _get_latest_rc_tag_name < <(list_release_tags "${full_version}")
}

function _get_max_major_version {
  max_major=-1
  while read -r release_branch_name ; do
    _major="$(major_version_from_branch_name "${release_branch_name}")"
    [[ $_major -gt $max_major ]] && max_major=$_major
  done
  echo "${max_major}"
}

function get_max_major_version {
  _get_max_major_version < <(list_release_branches "")
}

function _get_max_minor_version {
  max_minor=-1
  while read -r release_branch_name ;do
    _minor="$(minor_version_from_branch_name "${release_branch_name}")"
    [[ $_minor -gt $max_minor ]] && max_minor=$_minor
  done
  echo "${max_minor}"
}

function get_max_minor_version {
  _get_max_minor_version < <(list_release_branches "${version_major}")
}
