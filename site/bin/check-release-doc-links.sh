#!/usr/bin/env bash
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

set -euo pipefail

cd "$(dirname "$0")/../.."

# This check is intentionally scoped to user-facing content and guide-rendered output.
# It excludes known developer-oriented areas such as:
# - site/content/in-dev/**
# - site/content/releases/**
# - site/content/community/release-guides/**
# - the explicit developer-docs link on site/content/docs/_index.md
#
# The goal is to keep ordinary public entry points and guides from drifting back to
# unreleased docs or raw main-branch source links.
readonly pattern='https://polaris\.apache\.org/in-dev/unreleased|/in-dev/unreleased/|github\.com/apache/polaris/tree/main'

matches="$(
  rg -n --no-heading --color=never \
    "${pattern}" \
    README.md \
    site/content/_index.adoc \
    site/content/blog \
    site/content/guides \
    site/content/tools \
    site/layouts/guides/baseof.html || true
)"

if [[ -n "${matches}" ]]; then
  echo "ERROR: found user-facing links to unreleased docs or raw main-branch sources:"
  echo
  echo "${matches}"
  echo
  echo "Replace these with released docs, download pages, or published guide assets."
  exit 1
fi

echo "Release-doc link check passed."
