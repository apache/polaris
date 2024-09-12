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

worktree_dir="$(realpath "${bin_dir}/../..")"
if [[ ! -f ${worktree_dir}/version.txt ]]; then
  echo "Looks like ${worktree_dir}/version.txt does not exist, aborting" > /dev/stderr
  exit 1
fi

# Constants
main_branch="releases-infra"
release_branch_prefix="release/"
release_branch_regex="^release\\/([0-9]+)[.](x|[0-9]+)$"
versioned_docs_branch="versioned-docs"
tag_prefix="apache-polaris-"
tag_regex="${tag_prefix}[0-9]+[.][0-9]+[.]([0-9]+)(-incubating)?(-rc([0-9]+))?"
# When going becoming a TLP, replace 'incubator/polaris' with 'polaris' !
project_release_dir_part="incubator/polaris"
svn_dist_dev_repo="https://dist.apache.org/repos/dist/dev/${project_release_dir_part}/"
svn_dist_release_repo="https://dist.apache.org/repos/dist/release/${project_release_dir_part}/"

svn_dir_dev="build/svn-source-dev"
svn_dir_release="build/svn-source-release"


cleanups=()

function exit_cleanup_trap {
  if [[ ${#cleanups[@]} -gt 0 ]]; then
    start_group "Post script cleanup"
    for cmd in "${cleanups[@]}"; do
      echo "Running cleanup: $cmd"
      $cmd
    done
    end_group
  fi
}

trap exit_cleanup_trap EXIT

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
