#!/usr/bin/env bash

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
# Build a Helm index.yaml file for Apache Polaris charts from both
# https://archive.apache.org/dist/incubator/polaris/helm-chart/ and
# https://downloads.apache.org/incubator/polaris/helm-chart/
#
# The current release workflows force a regeneration of the Helm index
# after the old (unmaintained) releases are removed from the Apache dist
# release SVN server.  This causes previous releases to be removed from
# the Helm index (#3500).
#
# This script is a tentative fix that re-generates a Helm index across
# downloads.apache.org as well as archive.apache.org.  The logic is as
# follows:
#
# 1. All Helm charts from downloads.a.o and archive.a.o are downloaded
#    locally so that the index is fully rebuilt across all versions.
# 2. The URL of any Helm chart that is located on archive.apache.org but
#    not on downloads.apache.org is replaced by an absolute URL to
#    archive.apache.org.  That way, previous releases are always
#    accessible.
# 3. The URL of any Helm chart that is located on downloads.apache.org is
#    replaced by a relative path `${version}/polaris-${version}.tgz`.
#
# That way, the Helm index can be rebuild before a release vote is
# started, as the relative path will work regardless of the location being
# used (dist dev during RC vote, dist release and downloads.a.o after
# vote).
#

set -euo pipefail

# Function to extract version directories from an Apache directory listing
get_helm_chart_version_from_url() {
  local base_url="$1"
  curl -sL "$base_url/" |
    grep -oE 'href="[0-9]+\.[0-9]+\.[0-9]+-incubating/"' |
    sed 's/href="//;s/\/"//' |
    sort -V | uniq
}

# Base URLs for Apache Polaris helm charts
ARCHIVE_URL="https://archive.apache.org/dist/incubator/polaris/helm-chart"
DOWNLOADS_URL="https://downloads.apache.org/incubator/polaris/helm-chart"

# Temporary directory for downloads
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

echo "Working directory: $WORK_DIR"

# Collect all unique versions from both sources
echo "Discovering chart versions from archive.apache.org..."
ARCHIVE_VERSIONS=$(get_helm_chart_version_from_url "$ARCHIVE_URL" || echo "")
echo "Found archive versions: $ARCHIVE_VERSIONS"

echo "Discovering chart versions from downloads.apache.org..."
DOWNLOADS_VERSIONS=$(get_helm_chart_version_from_url "$DOWNLOADS_URL" || echo "")
echo "Found downloads versions: $DOWNLOADS_VERSIONS"

# File to store version-to-URL mappings for archive
URL_MAP_FILE="$WORK_DIR/url_map.txt"
>"$URL_MAP_FILE"

# Process ARCHIVE versions first with absolute URL replacement
# Only process versions that are in archive but NOT in downloads
echo ""
echo "Processing versions from archive.apache.org that are not in downloads.apache.org..."
for version in $ARCHIVE_VERSIONS; do
  # Skip if this version is also in downloads
  if echo "$DOWNLOADS_VERSIONS" | grep -q "^${version}$"; then
    echo "Skipping archive version $version (available in downloads.apache.org)"
    continue
  fi

  echo "Processing archive version: $version"

  CHART_URL="${ARCHIVE_URL}/${version}/polaris-${version}.tgz"
  LOCAL_TGZ="$WORK_DIR/polaris-${version}.tgz"

  # Store the mapping: local filename -> remote URL for archive
  echo "polaris-${version}.tgz|${CHART_URL}" >>"$URL_MAP_FILE"

  echo "  Downloading: $CHART_URL"
  if ! curl -sLf -o "$LOCAL_TGZ" "$CHART_URL"; then
    echo "  WARNING: Failed to download $CHART_URL, skipping..."
    continue
  fi
  echo "  Done."
done

# Process DOWNLOADS versions without URL replacement
echo ""
echo "Processing versions from downloads.apache.org..."
for version in $DOWNLOADS_VERSIONS; do
  echo "Processing downloads version: $version"

  CHART_URL="${DOWNLOADS_URL}/${version}/polaris-${version}.tgz"
  LOCAL_TGZ="$WORK_DIR/polaris-${version}.tgz"

  # Store the mapping: local filename -> relative URL for downloads
  echo "polaris-${version}.tgz|${version}/polaris-${version}.tgz" >>"$URL_MAP_FILE"

  echo "  Downloading: $CHART_URL"
  if ! curl -sLf -o "$LOCAL_TGZ" "$CHART_URL"; then
    echo "  WARNING: Failed to download $CHART_URL, skipping..."
    continue
  fi
  echo "  Done."
done

# Use helm to generate the index
echo ""
echo "Generating index.yaml with helm repo index..."
helm repo index "$WORK_DIR"

# Post-process the index.yaml to replace local paths with remote URLs
echo "Updating URLs to point to remote sources..."

while IFS='|' read -r local_name remote_url; do
  # Use sed to replace the local filename with the full remote URL
  sed -i.bak "s|^\( *- \)${local_name}$|\1${remote_url}|" "$WORK_DIR/index.yaml"
done <"$URL_MAP_FILE"

# Copy the updated index.yaml to the current directory
cp "$WORK_DIR/index.yaml" ./index.yaml

echo ""
echo "Successfully generated index.yaml"
ARCHIVE_COUNT=$(echo "$ARCHIVE_VERSIONS" | grep -v '^$' | wc -w | tr -d ' ')
DOWNLOADS_COUNT=$(echo "$DOWNLOADS_VERSIONS" | grep -v '^$' | wc -w | tr -d ' ')
echo "Charts indexed from archive: $ARCHIVE_COUNT"
echo "Charts indexed from downloads: $DOWNLOADS_COUNT"
