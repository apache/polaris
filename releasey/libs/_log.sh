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
# Common Logging Functions
#

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RESET='\033[0m' # No Color

function print_error() {
  echo -e "${RED}ERROR: $*${RESET}" >&2
}

function print_success() {
  echo -e "${GREEN}SUCCESS: $*${RESET}" >&2
}

function print_warning() {
  echo -e "${YELLOW}WARNING: $*${RESET}" >&2
}

function print_info() {
  echo "INFO: $*" >&2
}

function print_command() {
  # This function can be used to print the bash commands that are executed by a
  # script.  It either prints its arguments as-is to file descriptor 3, if it
  # is open, or prepends "DEBUG: " and prints them to stdout if it is unset.
  #
  # This allows the programmatic verification of all commands that are executed
  # by a script, by running it as follows:
  #
  #   $ ./script.sh 3>/tmp/script.log
  #   $ cat /tmp/script.log
  #   ./gradlew clean build
  #   rm -rf ./regtests/output
  #   mkdir -p ./regtests/output
  #   chmod -R 777 ./regtests/output
  #
  if { >&3; } 2>/dev/null; then
    echo "$*" >&3
  else
    echo -e "${BLUE}DEBUG: $*${RESET}" >&2
  fi
}
