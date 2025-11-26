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

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Git/SVN repository constants
APACHE_DIST_URL=${APACHE_DIST_URL:-"https://dist.apache.org/repos/dist"}
APACHE_DIST_PATH=${APACHE_DIST_PATH:-"/dev/incubator/polaris"}

# Execution mode constants
DRY_RUN=${DRY_RUN:-1}

# Version validation regex patterns
VERSION_REGEX="([0-9]+)\.([0-9]+)\.([0-9]+)-incubating"
VERSION_REGEX_GIT_TAG="^apache-polaris-$VERSION_REGEX-rc([0-9]+)$"
# Branch validation regex pattern for major.minor.x format
BRANCH_VERSION_REGEX="([0-9]+)\.([0-9]+)\.x"

# Project file constants
VERSION_FILE="$LIBS_DIR/../../version.txt"
CHANGELOG_FILE="$LIBS_DIR/../../CHANGELOG.md"
HELM_CHART_YAML_FILE="$LIBS_DIR/../../helm/polaris/Chart.yaml"
HELM_README_FILE="$LIBS_DIR/../../helm/polaris/README.md"
