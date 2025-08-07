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

releasey_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIBS_DIR="${releasey_dir}/libs"

source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"
source "${LIBS_DIR}/_version.sh"
source "${LIBS_DIR}/_gpg.sh"

function usage() {
  cat << EOF
$(basename "$0") [--help | -h]

  Builds source and binary distributions and stages them to the Apache dist dev repository.
  The version is automatically determined from the current git tag.  I.e. this script must
  be run from a release candidate tag.

  Options:
    -h --help
        Print usage information.

  Examples:
    $(basename "$0")

EOF
}

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
  print_error "Current HEAD is not on a release candidate tag. Please checkout a release candidate tag first."
  print_error "Use: git checkout apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi
print_info "Found git tag: ${git_tag}"

# Validate git tag format and extract version components
if ! validate_and_extract_git_tag_version "${git_tag}"; then
  print_error "Invalid git tag format: ${git_tag}"
  print_error "Expected format: apache-polaris-x.y.z-incubating-rcN"
  exit 1
fi

polaris_version="${major}.${minor}.${patch}-incubating"

print_info "Starting build and stage distributions process..."
print_info "Version: ${polaris_version}"
print_info "RC number: ${rc_number}"
echo

# Build distributions
print_info "Building source and binary distributions..."
exec_process cd "${releasey_dir}/.."
exec_process ./gradlew build sourceTarball -Prelease -PuseGpgAgent -x test -x intTest

# Create Helm package
print_info "Creating Helm package..."
exec_process cd "${releasey_dir}/../helm"
exec_process helm package polaris
exec_process helm gpg sign polaris-${polaris_version}.tgz
calculate_sha512 polaris-${polaris_version}.tgz polaris-${polaris_version}.tgz.sha512
exec_process gpg --armor --output polaris-${polaris_version}.tgz.asc --detach-sig polaris-${polaris_version}.tgz
calculate_sha512 polaris-${polaris_version}.tgz.prov polaris-${polaris_version}.tgz.prov.sha512
exec_process gpg --armor --output polaris-${polaris_version}.tgz.prov.asc --detach-sig polaris-${polaris_version}.tgz.prov

# Stage to Apache dist dev repository
print_info "Staging artifacts to Apache dist dev repository..."

dist_dev_dir=${releasey_dir}/polaris-dist-dev
print_info "Checking out ${APACHE_DIST_URL}${APACHE_DIST_PATH} to ${dist_dev_dir}..."
exec_process svn co "${APACHE_DIST_URL}${APACHE_DIST_PATH}" "${dist_dev_dir}"

print_info "Copying files to destination directory..."
version_dir="${dist_dev_dir}/${polaris_version}"
helm_chart_version_dir="${dist_dev_dir}/helm-chart/${polaris_version}"
exec_process mkdir -p "${version_dir}"
exec_process mkdir -p "${helm_chart_version_dir}"
exec_process cp ${releasey_dir}/../build/distributions/apache-polaris-${polaris_version}.tar.gz* "${version_dir}/"
exec_process cp ${releasey_dir}/../runtime/distribution/build/distributions/* "${version_dir}/"

print_info "Copying Helm package files..."
exec_process cp ${releasey_dir}/../helm/polaris-${polaris_version}.tgz* "${helm_chart_version_dir}/"

print_info "Adding files to SVN..."
exec_process svn add "${version_dir}"
exec_process svn add "${helm_chart_version_dir}"

print_info "Committing changes..."
exec_process svn commit -m "Stage Apache Polaris ${polaris_version} RC${rc_number}"

print_info "Updating Helm index..."
exec_process cd "${dist_dev_dir}/helm-chart"
exec_process helm repo index .
exec_process cd "${releasey_dir}/.."

# Publish Maven artifacts
print_info "Publishing Maven artifacts to Apache staging repository..."
exec_process ./gradlew publishToApache -Prelease -PuseGpgAgent -Dorg.gradle.parallel=false

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
