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

# Colors for output - only use colors if terminal supports them
if [[ -t 2 ]] &&
   [[ "${NO_COLOR:-}" != "1" ]] &&
   [[ "${TERM:-}" != "dumb" ]] &&
   command -v tput >/dev/null; then
  RED=${RED:-$(tput setaf 1)}
  GREEN=${GREEN:-$(tput setaf 2)}
  YELLOW=${YELLOW:-$(tput bold; tput setaf 3)}
  BLUE=${BLUE:-$(tput setaf 4)}
  RESET=${RESET:-$(tput sgr0)}
else
  RED=${RED:-''}
  GREEN=${GREEN:-''}
  YELLOW=${YELLOW:-''}
  BLUE=${BLUE:-''}
  RESET=${RESET:-''}
fi

function print_error() {
  echo -e "${RED}ERROR: $*${RESET}" >&2
}

function print_warning() {
  echo -e "${YELLOW}WARNING: $*${RESET}" >&2
}

function print_info() {
  echo "INFO: $*" >&2
}

function print_success() {
  echo -e "${GREEN}SUCCESS: $*${RESET}" >&2
}

function print_command() {
  # This function prints the bash commands that are executed by a script
  # with a DEBUG prefix to stderr for visibility.
  echo -e "${BLUE}DEBUG: $*${RESET}" >&2
}
