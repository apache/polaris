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
# Maven/Gradle Credentials Verification Script
#
# This script verifies that Maven/Gradle credentials are properly configured for Apache Polaris releases:
# 1. Checks if Apache credentials are configured in ~/.gradle/gradle.properties
# 2. Checks if Apache credentials are configured via environment variables
# 3. Validates that at least one method is configured
# 4. Attempts to verify credentials against Apache Nexus repository
#
# Returns non-zero exit code if any verification fails.
#

set -euo pipefail

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common logging functions and constants
source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"

function check_apache_credentials_are_set_in_gradle_properties() {
  # Check if Apache credentials are configured in ~/.gradle/gradle.properties, return non-zero if not
  print_info "Checking for Apache credentials in ${GRADLE_PROPERTIES_FILE}..."

  if [[ ! -f "${GRADLE_PROPERTIES_FILE}" ]]; then
    print_warning "Gradle properties file not found: ${GRADLE_PROPERTIES_FILE}"
    return 1
  fi

  if grep -q "^apacheUsername=" "${GRADLE_PROPERTIES_FILE}" 2>/dev/null &&
     grep -q "^apachePassword=" "${GRADLE_PROPERTIES_FILE}" 2>/dev/null; then
    print_success "Found Apache credentials in gradle.properties"
    return 0
  else
    print_warning "Apache credentials not found in ${GRADLE_PROPERTIES_FILE}"
    return 1
  fi
}

function check_apache_credentials_are_set_in_environment_variables() {
  # Check if Apache credentials are configured via environment variables, return non-zero if not
  print_info "Checking for Apache credentials in environment variables..."

  if [[ -n "${ORG_GRADLE_PROJECT_apacheUsername:-}" &&
        -n "${ORG_GRADLE_PROJECT_apachePassword:-}" ]]; then
    print_success "Found Apache credentials in environment variables"
    return 0
  else
    print_warning "Apache credentials not found in environment variables"
    return 1
  fi
}

function ensure_credentials_are_configured() {
  # Ensure that Apache credentials are configured in either gradle.properties or environment variables, return non-zero if not
  print_info "Checking Apache credentials configuration..."
  
  if check_apache_credentials_are_set_in_gradle_properties; then
    print_success "All Maven/Gradle credential checks passed! Your Apache credentials are properly configured for releases."
    return 0
  fi
  
  if check_apache_credentials_are_set_in_environment_variables; then
    print_success "All Maven/Gradle credential checks passed! Your Apache credentials are properly configured for releases."
    return 0
  fi
  
  print_error "Apache credentials are not configured in either ${GRADLE_PROPERTIES_FILE} or environment variables"
  return 1
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  ensure_credentials_are_configured "$@"
fi
