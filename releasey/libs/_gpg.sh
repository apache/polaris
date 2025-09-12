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
# GPG Setup Verification Script
#
# This script verifies that GPG is properly configured for Apache Polaris releases:
# 1. Checks that GPG is installed and available in PATH
# 1. Checks that the signing key configured in ~/.gradle/gradle.properties exists in the local GPG keyring
# 2. Verifies that the key has been published to hkps://keyserver.ubuntu.com
# 3. Confirms that the key is part of the KEYS file
#
# Returns non-zero exit code if any verification fails.
#

set -euo pipefail

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common logging functions and constants
source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"

function get_secret_keys() {
  # Get list of secret keys (private keys) that can be used for signing
  gpg --list-secret-keys --with-colons 2>/dev/null | grep '^sec:' | cut -d: -f5
}

function get_gradle_signing_key_id() {
  # Get the signing key ID from gradle.properties
  grep '^signing.gnupg.keyName' "${GRADLE_PROPERTIES_FILE}" 2>/dev/null | cut -d'=' -f2
}

function get_key_info() {
  # Get specific key information from keyring, including user ID, return non-zero if not found
  local key_id="$1"

  gpg --list-keys --with-colons "${key_id}" 2>/dev/null | grep '^uid:' | head -n1 | cut -d: -f10 || return 1
}

function ensure_gpg_is_available() {
  # Check if GPG is installed, return non-zero if not
  print_info "Checking if GPG is available..."
  if ! command -v gpg >/dev/null 2>&1; then
    print_error "GPG is not installed or not in PATH"
    return 1
  fi
  print_success "GPG is available: $(gpg --version | head -n1)"
}

function ensure_signing_key_exists() {
  # Check if there is at least one secret key in the GPG keyring, return non-zero if not
  # Store the first key for the rest of the script in the SIGNING_KEY_ID variable
  print_info "Checking for GPG keys in local keyring..."
  
  local secret_keys
  secret_keys=$(get_secret_keys)

  if [[ -z "${secret_keys}" ]]; then
    print_error "No secret keys found in GPG keyring"
    print_error "You need to have a GPG key pair for signing releases"
    return 1
  fi

  local key_count
  key_count=$(echo "${secret_keys}" | wc -l | tr -d ' ')
  print_info "Found ${key_count} secret key(s) in local GPG keyring"

  SIGNING_KEY_ID=$(get_gradle_signing_key_id)
  if [[ -z "${SIGNING_KEY_ID}" ]]; then
    print_warning "No signing key found in ${GRADLE_PROPERTIES_FILE}"
    return 1
  fi

  local key_info
  key_info=$(get_key_info "${SIGNING_KEY_ID}")
  if [[ -z "${key_info}" ]]; then
    print_error "Signing key ${SIGNING_KEY_ID} not found in local GPG keyring"
    return 1
  fi

  print_success "Using key: ${SIGNING_KEY_ID} ($(get_key_info "${SIGNING_KEY_ID}"))"
  
  return 0
}

function ensure_signing_key_is_published() {
  # Check if the key associated with $SIGNING_KEY_ID is published on the keyserver, return non-zero if not
  print_info "Checking if key ${SIGNING_KEY_ID} is published on ${KEYSERVER}..."

  # Try to receive the key from the keyserver
  if exec_process gpg --keyserver "${KEYSERVER}" --recv-keys "${SIGNING_KEY_ID}" >/dev/null; then
    print_success "Key ${SIGNING_KEY_ID} found on keyserver ${KEYSERVER}"
    return 0
  else
    print_error "Key ${SIGNING_KEY_ID} not found on keyserver ${KEYSERVER}"
    print_error "Publish your key with: gpg --keyserver ${KEYSERVER} --send-keys ${SIGNING_KEY_ID}"
    return 1
  fi
}

function ensure_signing_key_is_in_keys_file() {
  # Check if the key associated with $SIGNING_KEY_ID is in the KEYS file, return non-zero if not
  print_info "Checking if key ${SIGNING_KEY_ID} is in the KEYS file..."

  # Download the KEYS file
  local keys_content
  if ! keys_content=$(exec_process curl -s "${KEYS_URL}"); then
    print_error "Failed to download KEYS file from ${KEYS_URL}"
    return 1
  fi
  
  # Check if the key is in the KEYS file
  if [[ ${DRY_RUN:-1} -eq 1 ]] ||
     echo "${keys_content}" | grep -q "${SIGNING_KEY_ID}"; then
    print_success "Key ${SIGNING_KEY_ID} found in KEYS file"
    return 0
  else
    print_error "Key ${SIGNING_KEY_ID} not found in KEYS file"
    print_error "Add your key to the KEYS file following the release guide instructions"
    print_error "See: ${KEYS_URL}"
    return 1
  fi
}

function ensure_gpg_setup_is_done() {
  # Check 1: GPG availability
  if ! ensure_gpg_is_available; then
    return 1
  fi
  
  # Check 2: Local GPG keys
  if ! ensure_signing_key_exists; then
    return 1
  fi

  # Check 3: Key on keyserver
  if ! ensure_signing_key_is_published; then
    return 1
  fi

  # Check 4: Key in KEYS file
  if ! ensure_signing_key_is_in_keys_file; then
    return 1
  fi

  print_success "All GPG setup checks passed! Your GPG setup is ready for releases."
}

function calculate_sha512 {
  local source_file="$1"
  local target_file="$2"
  # This function is only there for dry-run support.  Because of the
  # redirection, we cannot use exec_process with the exact command that will be
  # executed.
  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    exec_process shasum -a 512 "${source_file}" > "${target_file}"
  else
    exec_process "shasum -a 512 ${source_file} > ${target_file}"
  fi
}


# Global variable to store the signing key ID
SIGNING_KEY_ID=""

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  ensure_gpg_setup_is_done "$@"
fi
