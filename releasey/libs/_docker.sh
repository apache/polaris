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
# Docker Setup Verification Script
#
# This script verifies that Docker is properly configured for Apache Polaris releases:
# 1. Checks that Docker is installed and available in PATH
# 2. Verifies that Docker daemon is running and accessible
# 3. Confirms that Docker buildx is available for multi-platform builds
# 4. Checks Docker Hub login status (optional)
#
# Returns non-zero exit code if any verification fails.
#

set -euo pipefail

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common logging functions and constants
source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"

function ensure_docker_is_available() {
  # Check if Docker is installed, return non-zero if not
  print_info "Checking if Docker is available..."
  if ! command -v docker >/dev/null 2>&1; then
    print_error "Docker is not installed or not in PATH"
    print_error "Please install Docker to build and publish Docker images"
    return 1
  fi
  print_success "✓ Docker is installed"
  return 0
}

function ensure_docker_daemon_is_accessible() {
  # Check if Docker daemon is running and accessible, return non-zero if not
  print_info "Checking if Docker daemon is accessible..."
  if ! docker info >/dev/null 2>&1; then
    print_error "Docker daemon is not running or not accessible"
    print_error "Please start Docker daemon and ensure you have permissions to access it"
    return 1
  fi
  print_success "✓ Docker daemon is accessible"
  return 0
}

function ensure_docker_buildx_is_available() {
  # Check if Docker buildx is available, return non-zero if not
  print_info "Checking if Docker buildx is available..."
  if ! docker buildx version >/dev/null 2>&1; then
    print_error "Docker buildx is not available"
    print_error "Please install Docker with buildx support for multi-platform builds"
    return 1
  fi
  print_success "✓ Docker buildx is available"
  return 0
}

function check_docker_hub_login_status() {
  # Check if user is logged in to Docker Hub (optional check)
  print_info "Checking Docker Hub login status..."
  if docker info 2>/dev/null | grep -q "Username:"; then
    local username
    username=$(docker info 2>/dev/null | grep "Username:" | awk '{print $2}')
    print_success "✓ Logged in to Docker Hub as: ${username}"
    return 0
  else
    print_warning "⚠ Not logged in to Docker Hub"
    print_warning "  You may need to run 'docker login' before publishing images"
    return 0 # This is not a failure, just a warning
  fi
}

function ensure_docker_setup_is_done() {
  print_info "Verifying Docker setup..."

  # Check 1: Docker availability
  if ! ensure_docker_is_available; then
    return 1
  fi

  # Check 2: Docker daemon accessibility
  if ! ensure_docker_daemon_is_accessible; then
    return 1
  fi

  # Check 3: Docker buildx availability
  if ! ensure_docker_buildx_is_available; then
    return 1
  fi

  # Check 4: Docker Hub login status (warning only)
  check_docker_hub_login_status

  return 0
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  ensure_docker_setup_is_done "$@"
fi
