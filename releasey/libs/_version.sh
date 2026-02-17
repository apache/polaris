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
# Utility functions for version validation and version.txt manipulation
#

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$LIBS_DIR/_constants.sh"
source "$LIBS_DIR/_exec.sh"

function validate_and_extract_branch_version {
  # This function validates the format of a release branch version and extracts its components (major.minor).
  # It now accepts the major.minor.x format (e.g., "1.1.x") instead of exact version format.
  # It returns 0 if the version is valid and sets the global variables major, minor.
  # The patch version is not extracted from the branch name as it uses the "x" placeholder.
  # Otherwise, it returns 1.
  local version="$1"

  if [[ ! ${version} =~ ${BRANCH_VERSION_REGEX} ]]; then
    return 1
  fi

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  # patch is not set from branch name since it uses "x" placeholder

  return 0
}

function validate_and_extract_git_tag_version {
  # This function validates the format of a git tag version and extracts its components (major.minor.patch and rc number).
  # It is similar to validate_and_extract_rc_version, but for git tag format.
  # It returns 0 if the version is valid and sets the global variables major, minor, patch, and rc_number.
  # It also sets the global variable version_without_rc to the "major.minor.patch" format without the rc number.
  # Otherwise, it returns 1.
  local version="$1"

  if [[ ! ${version} =~ ${VERSION_REGEX_GIT_TAG} ]]; then
    return 1
  fi

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  rc_number="${BASH_REMATCH[4]}"
  version_without_rc="${major}.${minor}.${patch}"

  return 0
}

function validate_and_extract_polaris_version {
  # This function validates the format of a Polaris version and extracts its components (major.minor.patch).
  # It accepts the full version format (e.g., "1.0.0") and sets the global variables major, minor, patch.
  # It also sets the global variable version_without_rc to the "major.minor.patch" format.
  # Returns 0 if the version is valid, 1 otherwise.
  local version="$1"

  if [[ ! ${version} =~ ${VERSION_REGEX} ]]; then
    return 1
  fi

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  version_without_rc="${major}.${minor}.${patch}"

  return 0
}

function update_version {
  local version="$1"
  update_version_txt "${version}"
  update_helm_version "${version}"
}

function update_version_txt {
  local version="$1"
  # This function is only there for dry-run support.  Because of the
  # redirection, we cannot use exec_process with the exact command that will be
  # executed.
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    exec_process echo "${version}" >$VERSION_FILE
  else
    exec_process "echo ${version} > $VERSION_FILE"
  fi
}

function update_helm_version {
  local new_version="$1"
  exec_process sed -E -i~ "s/^version: .+/version: ${new_version}/g" "$HELM_CHART_YAML_FILE"
  exec_process sed -E -i~ "s/^appVersion: .+/appVersion: ${new_version}/g" "$HELM_CHART_YAML_FILE"
  exec_process sed -E -i~ "/- name: Documentation/{n;s|url: https://polaris.apache.org/$|url: https://polaris.apache.org/releases/${new_version}/|;}" "$HELM_CHART_YAML_FILE"
  exec_process sed -E -i~ "s/[0-9]+[.][0-9]+([.][0-9]+)?(-incubating)?-SNAPSHOT/${new_version}/g" "$HELM_README_FILE"
  # The readme file may contain version with double dash for shields.io badges
  # We need a second `sed` command to ensure that the version replacement preserves this double-dash syntax.
  local current_version_with_dash
  local version_with_dash
  current_version_with_dash="${old_version//-/--}"
  version_with_dash="${version//-/--}"
  exec_process sed -E -i~ "s/[0-9]+[.][0-9]+([.][0-9]+)?(--incubating)?--SNAPSHOT/${version_with_dash}/g" "$HELM_README_FILE"
  exec_process sed -E -i~ "s|/in-dev/unreleased/|/releases/${new_version}/|g" "$HELM_VALUES_FILE"
  exec_process sed -E -i~ "s|/in-dev/unreleased/|/releases/${new_version}/|g" "$HELM_VALUES_SCHEMA_FILE"
  exec_process sed -E -i~ 's/^(  tag: )"latest".*$/\1"'"${new_version}"'"/' "$HELM_VALUES_FILE"
}

function find_next_rc_number {
  # This function finds the next available RC number for a given version.
  # It returns 0 and sets the global variable rc_number to the next available RC number.
  # RC numbers start from 0. It takes the version_without_rc as input (e.g., "1.0.0").
  local version_without_rc="$1"

  # Get all existing RC tags for this version
  local tag_pattern="apache-polaris-${version_without_rc}-rc*"
  local existing_tags
  existing_tags=$(git tag -l "${tag_pattern}" | sort -V)

  if [[ -z "${existing_tags}" ]]; then
    # No existing RC tags, start with RC0
    rc_number=0
  else
    # Extract the highest RC number and increment
    local highest_rc
    highest_rc=$(echo "${existing_tags}" | sed "s/apache-polaris-${version_without_rc}-rc//" | sort -n | tail -1)
    rc_number=$((highest_rc + 1))
  fi

  return 0
}

function find_next_patch_number {
  # This function finds the next available patch number for a given major.minor version.
  # It returns 0 and sets the global variable patch to the next available patch number.
  # Patch numbers start from 0. It takes major and minor as input (e.g., "1", "0").
  #
  # The patch number should only be incremented if there is a final release tag (without -rc suffix)
  # for the current highest patch. If only RC tags exist for the highest patch, we should reuse
  # that patch number (allowing for additional RCs like rc1, rc2, etc.).
  local major="$1"
  local minor="$2"

  # Get all existing RC tags for this major.minor version
  local rc_tag_pattern="apache-polaris-${major}.${minor}.*-rc*"
  local existing_rc_tags
  existing_rc_tags=$(git tag -l "${rc_tag_pattern}" | sort -V)

  if [[ -z "${existing_rc_tags}" ]]; then
    # No existing RC tags, start with patch 0
    patch=0
  else
    # Extract all patch numbers from RC tags and find the highest
    local highest_patch=-1
    while IFS= read -r tag; do
      if [[ ${tag} =~ apache-polaris-${major}\.${minor}\.([0-9]+)-rc[0-9]+ ]]; then
        local current_patch="${BASH_REMATCH[1]}"
        if [[ ${current_patch} -gt ${highest_patch} ]]; then
          highest_patch=${current_patch}
        fi
      fi
    done <<< "${existing_rc_tags}"

    # Check if a final release tag exists for the highest patch (without -rc suffix)
    local final_tag="apache-polaris-${major}.${minor}.${highest_patch}"
    if git rev-parse "${final_tag}" >/dev/null 2>&1; then
      # Final release tag exists, increment to next patch number
      patch=$((highest_patch + 1))
    else
      # No final release tag yet, reuse the same patch number for additional RCs
      patch=${highest_patch}
    fi
  fi

  return 0
}