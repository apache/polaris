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
# Create Release Candidate Tag Script
#
# Automates the "Create release candidate tag" section of the release guide.
#

set -euo pipefail

releases_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
libs_dir="${releases_dir}/libs"

source "${libs_dir}/_log.sh"
source "${libs_dir}/_constants.sh"
source "${libs_dir}/_exec.sh"
source "${libs_dir}/_version.sh"

function usage() {
  cat << EOF
$(basename "$0") --version VERSION [--help | -h]

  Creates a release candidate tag for a release candidate.

  Options:
    --version VERSION
        The release version in format x.y.z-incubating-rcN where N is the RC number
    -h --help
        Print usage information.

  Examples:
    $(basename "$0") --version 1.0.0-incubating-rc1
    $(basename "$0") --version 0.1.0-incubating-rc2

EOF
}

ensure_cwd_is_project_root

version=""

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

# Validate version format: x.y.z-incubating-rcN
# TODO: Remove incubating when we are a TLP
if ! validate_and_extract_rc_version "${version}"; then
  print_error "Invalid version format. Expected: x.y.z-incubating-rcN where N>0, got: ${version}"
  usage >&2
  exit 1
fi

# Define polaris_version from extracted components
polaris_version="${major}.${minor}.${patch}-incubating"

print_info "Starting release candidate tag creation..."
print_info "Version: ${version}"
print_info "Polaris version: ${polaris_version}"
print_info "RC number: ${rc_number}"
echo

# If we're creating RC2 or later, verify previous RC tag exists
if [[ ${rc_number} -gt 1 ]]; then
  previous_rc=$((rc_number - 1))
  previous_tag="apache-polaris-${version%-rc*}-rc${previous_rc}"
  
  print_info "Checking for previous RC tag: ${previous_tag}"
  if ! git tag -l "${previous_tag}" | grep -q "^${previous_tag}$"; then
    print_error "Previous RC tag ${previous_tag} does not exist. Cannot create RC${rc_number} without RC${previous_rc}."
    exit 1
  fi
  print_info "Previous RC tag ${previous_tag} found"
fi

# Create the release candidate tag
release_tag="apache-polaris-${version}"

# Check if tag already exists
if git tag -l "${release_tag}" | grep -q "^${release_tag}$"; then
  print_error "Release Candidate tag ${release_tag} already exists"
  exit 1
fi

print_info "Creating release candidate tag: ${release_tag}"
exec_process git tag "${release_tag}"

print_info "Pushing release candidate tag to ${APACHE_REMOTE_NAME}"
exec_process git push "${APACHE_REMOTE_NAME}" "${release_tag}"

print_info "Checking out release candidate tag"
exec_process git checkout "${release_tag}"

echo
print_success "🎉 Release Candidate tag ${release_tag} created successfully!"
echo
print_info "Next steps:"
print_info "1. (Optional) Use the script 04-build-and-test.sh to build the release and run the regression tests."
print_info "2. Build and stage the distributions"
print_info "3. Start the vote thread"
