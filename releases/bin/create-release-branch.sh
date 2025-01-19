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
command_name="$(basename "$0")"
. "${bin_dir}/_releases_lib.sh"

echo "Version in version.txt is '$version_txt'"
echo "Current branch is '${current_branch}' on remote '${upstream_name}'"
echo ""

function usage {
  cat << EOF
${command_name} [--major MAJOR_VERSION] [--minor MINOR_VERSION] [--commit GIT_COMMIT] [--recreate] [--dry-run] [--help | -h]

  Creates a new release branch using the pattern '${release_branch_prefix}/<major>.<minor>'.

  The major and minor versions are determined from the current branch.

  When invoked from the main branch, a new major-version branch '${release_branch_prefix}/<major>.x' will be created.
  When invoked from a major-version branch, a new minor-version branch '${release_branch_prefix}/<major>.<minor>' will be created.

  Options:
    --commit GIT_COMMIT
        The Git commit to draft the release from. Defaults to the current HEAD.
    --recreate
        Recreates the draft release if it already exists.
    --dry-run
        Do not update Git. Gradle will publish to the local Maven repository, but still sign the artifacts.
    -h --help
        Print usage information.
EOF
}

dry_run=
recreate=
create_from_commit="$(git rev-parse HEAD)"
while [[ $# -gt 0 ]]; do
  case $1 in
    --commit)
      shift
      if [[ $# -eq 0 ]]; then
        echo "Missing argument for --commit, aborting" > /dev/stderr
        exit 1
      fi
      create_from_commit="$1"
      shift
      ;;
    --recreate)
      recreate=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option/argument $1" > /dev/stderr
      usage > /dev/stderr
      exit 1
      ;;
  esac
done

new_branch_name=""
case "${branch_type}" in
  "main")
    max_major="$(get_max_major_version)"
    if [[ $max_major -eq -1 ]]; then
      echo "No major release branch found"
      new_branch_name="${release_branch_prefix}0.x"
    else
      echo "Latest major release branch is for version ${max_major}.x"
      new_branch_name="${release_branch_prefix}$(( $max_major + 1 )).x"
    fi
    ;;
  "major")
    max_minor="$(get_max_minor_version)"
    if [[ $max_minor -eq -1 ]]; then
      echo "No minor release branch found for ${version_major}.x"
      new_branch_name="${release_branch_prefix}${version_major}.0"
    else
      echo "Latest major release branch is for version ${max_major}.x"
      new_branch_name="${release_branch_prefix}${version_major}.$(( $max_minor + 1))"
    fi
    ;;
  "minor")
    echo "On a minor version branch, aborting" > /dev/stderr
    exit 1
    ;;
  *)
    echo "Unexpected branch type ${branch_type}" > /dev/stderr
    exit 1
esac

if [[ -z ${new_branch_name} ]]; then
  echo "Empty branch to create - internal error, aborting" > /dev/stderr
  exit 1
fi

do_recreate=
if [[ ${recreate} ]]; then
  if list_release_branches "" "" | grep --quiet "${new_branch_name}"; then
    do_recreate=1
  fi
fi

echo ""
if [[ ${dry_run} ]]; then
  echo "Dry run, no changes will be made"
else
  echo "Non-dry run, will update Git"
fi

echo ""
echo "New branch name: ${new_branch_name}"
echo "From commit:     ${create_from_commit}"
echo ""
git log -n1 "${create_from_commit}"
echo ""

[[ ${do_recreate} ]] && exec_process "${dry_run}" git branch -D "${new_branch_name}"
exec_process "${dry_run}" git checkout -b "${new_branch_name}" "${create_from_commit}"

[[ ${do_recreate} ]] && exec_process "${dry_run}" git push "${upstream_name}" --delete "${new_branch_name}"
exec_process "${dry_run}" git push --set-upstream "${upstream_name}"
