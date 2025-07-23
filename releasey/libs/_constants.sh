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

libs_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# GPG constants
KEYS_URL=${KEYS_URL:-"https://downloads.apache.org/incubator/polaris/KEYS"}
KEYSERVER=${KEYSERVER:-"hkps://keyserver.ubuntu.com"}

# Git/SVN repository constants
APACHE_REMOTE_NAME=${APACHE_REMOTE_NAME:-"apache"}
APACHE_REMOTE_URL=${APACHE_REMOTE_URL:-"https://github.com/apache/polaris.git"}
APACHE_DIST_URL=${APACHE_DIST_URL:-"https://dist.apache.org/repos/dist"}
APACHE_DIST_PATH=${APACHE_DIST_PATH:-"/dev/incubator/polaris"}

# Execution mode constants
DRY_RUN=${DRY_RUN:-1}

# Docker constants
DOCKER_REGISTRY=${DOCKER_REGISTRY:-"docker.io"}
DOCKER_HUB_URL=${DOCKER_HUB_URL:-"https://hub.docker.com"}

# Version validation regex patterns
VERSION_REGEX="([0-9]+)\.([0-9]+)\.([0-9]+)"
VERSION_REGEX_RELEASE="^$VERSION_REGEX-incubating$"
VERSION_REGEX_RC="^$VERSION_REGEX-incubating-rc([1-9][0-9]*)$"
VERSION_REGEX_GIT_TAG="^apache-polaris-$VERSION_REGEX-incubating-rc([0-9]+)$"

# Other project constants
GRADLE_PROPERTIES_FILE=${GRADLE_PROPERTIES_FILE:-"${HOME}/.gradle/gradle.properties"}
VERSION_FILE="$libs_dir/../../version.txt"
CHANGELOG_FILE="$libs_dir/../../CHANGELOG.md"