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
# Create Release Branch Script
#
# Automates the "Create release branch" section of the release guide.
# Creates a new release branch and sets the target release version.
#

set -euo pipefail

releases_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
libs_dir="${releases_dir}/libs"

source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_exec.sh"
source "${libs_dir}/_files.sh"

function usage() {
  cat << EOF
$(basename "$0") --version VERSION [--commit GIT_COMMIT] [--recreate] [--help | -h]

  Creates a release branch and sets the target release version.

  Options:
    --version VERSION
        The release version in format x.y.z-incubating
    --commit GIT_COMMIT
        The Git commit SHA to create the release branch from. Defaults to current HEAD.
    --recreate
        Drop the release branch if it already exists and recreate it to the new SHA.
    -h --help
        Print usage information.

  Examples:
    $(basename "$0") --version 1.0.0-incubating --commit HEAD
    $(basename "$0") --version 0.1.0-incubating --commit abc123def456
    $(basename "$0") --version 1.0.0-incubating --commit HEAD --recreate

EOF
}

ensure_cwd_is_project_root

version=""
commit="HEAD"
recreate=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      if [[ $# -lt 2 ]]; then
        print_error "Missing argument for --version"
        usage >&2
        exit 1
      fi
      version="$2"
      shift 2
      ;;
    --commit)
      if [[ $# -lt 2 ]]; then
        print_error "Missing argument for --commit"
        usage >&2
        exit 1
      fi
      commit="$2"
      shift 2
      ;;
    --recreate)
      recreate=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      print_error "Unknown option/argument $1"
      usage >&2
      exit 1
      ;;
  esac
done

# Ensure the version is provided
if [[ -z ${version} ]]; then
  print_error "Missing version"
  usage >&2
  exit 1
fi

# Validate that the commit exists
if ! git rev-parse --verify "${commit}" >/dev/null 2>&1; then
  print_error "Invalid Git commit: ${commit}"
  usage >&2
  exit 1
fi

# Validate version format: x.y.z-incubating
# TODO: Remove incubating when we are a TLP
version_regex="^([0-9]+)\.([0-9]+)\.([0-9]+)-incubating$"
if [[ ! ${version} =~ ${version_regex} ]]; then
  print_error "Invalid version format. Expected: x.y.z-incubating, got: ${version}"
  usage >&2
  exit 1
fi

# Extract version components
polaris_version="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"

print_info "Starting release branch creation..."
print_info "Version: ${version}"
print_info "Polaris version: ${polaris_version}"
print_info "From commit: ${commit}"
echo

release_branch="release/${polaris_version}"

# Check if release branch already exists
if git show-ref --verify --quiet "refs/heads/${release_branch}"; then
  if [[ "${recreate}" == "true" ]]; then
    print_info "Release branch ${release_branch} already exists, deleting it..."
    exec_process git branch -D "${release_branch}"
    if git show-ref --verify --quiet "refs/remotes/${APACHE_REMOTE_NAME}/${release_branch}"; then
      print_info "Deleting remote release branch from ${APACHE_REMOTE_NAME}..."
      exec_process git push "${APACHE_REMOTE_NAME}" --delete "${release_branch}"
    fi
  else
    print_error "Release branch ${release_branch} already exists. Use --recreate to delete and recreate it."
    exit 1
  fi
fi

print_info "Checking out commit: ${commit}"
exec_process git checkout "${commit}"

print_info "Creating release branch: ${release_branch}"
exec_process git branch "${release_branch}"

print_info "Pushing release branch to ${APACHE_REMOTE_NAME}"
exec_process git push "${APACHE_REMOTE_NAME}" "${release_branch}"

print_info "Checking out release branch"
exec_process git checkout "${release_branch}"

print_info "Setting version to ${polaris_version} in version.txt"
update_version "${polaris_version}"

print_info "Committing and pushing version change"
exec_process git add "$VERSION_FILE"
exec_process git commit -m "[chore] Bump version to ${polaris_version} for release"
exec_process git push "${APACHE_REMOTE_NAME}" "${release_branch}"

print_info "Updating CHANGELOG.md"
exec_process ./gradlew patchChangelog

print_info "Committing changelog change"
exec_process git add "$CHANGELOG_FILE"
exec_process git commit -m "[chore] Update changelog for release"
exec_process git push "${APACHE_REMOTE_NAME}" "${release_branch}"

echo
print_success "🎉 Release branch ${release_branch} created successfully!"
echo
print_info "Next steps:"
print_info "* Submit a PR on main branch to propagate CHANGELOG updates from the release branch to main (https://github.com/${APACHE_REMOTE_NAME}/polaris/pull/new/${release_branch})."
print_info "* Use the script 03-create-release-candidate-tag.sh to create a release tag."
