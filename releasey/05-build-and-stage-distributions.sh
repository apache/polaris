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
# Build and Stage Distributions Script
#
# Builds source and binary distributions and stages them to the Apache dist dev repository.
# This script automates the "Build and stage the distributions" step from the release guide.
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
$(basename "$0") [--help | -h]

  Builds source and binary distributions and stages them to the Apache dist dev repository.
  The version is automatically determined from the current git tag.  I.e. this script must
  be run from a release tag.

  Options:
    -h --help
        Print usage information.

  Examples:
    $(basename "$0")

EOF
}

ensure_cwd_is_project_root

while [[ $# -gt 0 ]]; do
  case $1 in
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

# Determine version from current git tag
print_info "Determining version from current git tag..."

if ! git_tag=$(git describe --tags --exact-match HEAD 2>/dev/null); then
  print_error "Current HEAD is not on a release tag. Please checkout a release tag first."
  print_error "Use: git checkout apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi
print_info "Found git tag: ${git_tag}"

# Extract version components from git tag in one regex match
git_tag_regex="^apache-polaris-([0-9]+)\.([0-9]+)\.([0-9]+)-incubating-rc([0-9]+)$"
if [[ ! ${git_tag} =~ ${git_tag_regex} ]]; then
  print_error "Invalid git tag format: ${git_tag}"
  print_error "Expected format: apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi

# Extract version components from regex match
major="${BASH_REMATCH[1]}"
minor="${BASH_REMATCH[2]}"
patch="${BASH_REMATCH[3]}"
rc_number="${BASH_REMATCH[4]}"
version="${major}.${minor}.${patch}-incubating"

print_info "Starting build and stage distributions process..."
print_info "Version: ${version}"
print_info "RC number: ${rc_number}"
echo

# Build distributions
print_info "Building source and binary distributions..."
exec_process ./gradlew build sourceTarball -Prelease -PuseGpgAgent -x test -x intTest

print_info "Verifying distribution files exist..."
source_dist_dir="build/distribution"
binary_dist_dir="runtime/distribution/build/distributions"
helm_chart_dir="helm/polaris"

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  if [[ ! -d "${source_dist_dir}" ]]; then
    print_error "Source distribution directory not found: ${source_dist_dir}"
    exit 1
  fi
  
  if [[ ! -d "${binary_dist_dir}" ]]; then
    print_error "Binary distribution directory not found: ${binary_dist_dir}"
    exit 1
  fi
  
  if [[ ! -d "${helm_chart_dir}" ]]; then
    print_error "Helm chart directory not found: ${helm_chart_dir}"
    exit 1
  fi
fi

# Stage to Apache dist dev repository
print_info "Staging artifacts to Apache dist dev repository..."

print_info "Checking out ${APACHE_DIST_URL}${APACHE_DIST_PATH}..."
dist_dev_dir="polaris-dist-dev"
exec_process svn co "${APACHE_DIST_URL}${APACHE_DIST_PATH}" "${dist_dev_dir}"

print_info "Copying files to destination directory..."
version_dir="${dist_dev_dir}/${version}"
helm_chart_version_dir="${dist_dev_dir}/helm-chart/${version}"
exec_process mkdir -p "${version_dir}"
exec_process mkdir -p "${helm_chart_version_dir}"
exec_process cp "${source_dist_dir}"/* "${version_dir}/"
exec_process cp "${binary_dist_dir}"/* "${version_dir}/"
exec_process cp -r "${helm_chart_dir}" "${helm_chart_version_dir}/"

print_info "Adding files to SVN..."
exec_process svn add "${version_dir}"
exec_process svn add "${helm_chart_version_dir}"

# Commit changes
print_info "Committing changes..."
exec_process svn commit -m "Stage Apache Polaris ${version} RC${rc_number}"

# Publish Maven artifacts
print_info "Publishing Maven artifacts to Apache staging repository..."
exec_process ./gradlew publishToApache -Prelease -PuseGpgAgent

echo
print_success "🎉 Distributions built and staged successfully!"
echo
print_info "Staged artifacts:"
print_info "- Source and binary distributions: ${version_dir}"
print_info "- Helm charts: ${helm_chart_version_dir}"
print_info "- Maven artifacts: Published to Apache staging repository"
echo
print_info "Next steps:"
print_info "1. Close the staging repository in Nexus"
print_info "2. Build and stage Docker images: ./06-build-and-stage-docker-images.sh"
print_info "3. Start the vote thread"
