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

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pushd "$SCRIPT_DIR/../python" > /dev/null

#############################
#      Delete old Tests     #
#############################

# List of test files to keep (from .../client/python)
KEEP_TESTS=(
  "test/test_cli_parsing.py"
)

find "test" -type f | while read -r file; do

  # Check if file should be excluded
  keep=false
  for test_path in "${KEEP_TESTS[@]}"; do
    if [[ "$file" == "$test_path"* ]]; then
      keep=true
    fi
  done

  if [ "$keep" = true ]; then
    echo "${file}: skipped"
    continue
  else
    echo "${file}: removed"
    rm "${file}"
  fi
done

#############################
# Regenerate Python clients #
#############################

echo "Regenerating python from the spec"

# TODO skip-validate-spec is needed because the upstream Iceberg spec seems invalid. e.g.:
#   [main] ERROR o.o.codegen.DefaultCodegen - Required var sort-order-id not in properties

OPEN_API_CLI_VERSION="v7.11.0"

docker run --rm \
  -v "${SCRIPT_DIR}/../..:/local" \
  openapitools/openapi-generator-cli:${OPEN_API_CLI_VERSION} generate \
  -i /local/spec/polaris-management-service.yml \
  -g python \
  -o /local/client/python \
  --additional-properties=packageName=polaris.management \
  --additional-properties=apiNamePrefix=polaris \
  --additional-properties=pythonVersion=3.9 \
  --ignore-file-override /local/client/python/.openapi-generator-ignore > /dev/null

docker run --rm \
  -v "${SCRIPT_DIR}/../..:/local" \
  openapitools/openapi-generator-cli:${OPEN_API_CLI_VERSION} generate \
  -i /local/spec/polaris-catalog-service.yaml \
  -g python \
  -o /local/client/python \
  --additional-properties=packageName=polaris.catalog \
  --additional-properties=apiNameSuffix="" \
  --skip-validate-spec \
  --additional-properties=pythonVersion=3.9 \
  --ignore-file-override /local/client/python/.openapi-generator-ignore > /dev/null

docker run --rm \
  -v "${SCRIPT_DIR}/../..:/local" \
  openapitools/openapi-generator-cli:${OPEN_API_CLI_VERSION} generate \
  -i /local/spec/iceberg-rest-catalog-open-api.yaml \
  -g python \
  -o /local/client/python \
  --additional-properties=packageName=polaris.catalog \
  --additional-properties=apiNameSuffix="" \
  --additional-properties=apiNamePrefix=Iceberg \
  --additional-properties=pythonVersion=3.9 \
  --skip-validate-spec \
  --ignore-file-override /local/client/python/.openapi-generator-ignore > /dev/null

#############################
#      Prepend Licenses     #
#############################

echo "Re-applying license headers"

prepend_header() {
  local file="$1"
  local header_file="$2"
  tmpfile=$(mktemp)
  cat "$header_file" "$file" > "$tmpfile"
  mv "$tmpfile" "$file"
}

# List of paths to exclude (from .../client/python)
EXCLUDE_PATHS=(
  "./.gitignore"
  "./.openapi-generator/"
  "./.openapi-generator-ignore"
  "./.pytest_cache"
  "./test/test_cli_parsing.py"
  "./cli/"
  "./polaris/__pycache__"
  "./polaris/catalog/__pycache__/"
  "./polaris/catalog/models/__pycache__/"
  "./polaris/catalog/api/__pycache__/"
  "./polaris/management/__pycache__/"
  "./polaris/management/models/__pycache__/"
  "./polaris/management/api/__pycache__/"
  "./integration_tests/"
  "./.github/workflows/python.yml"
  "./.gitlab-ci.yml"
  "./pyproject.toml"
  "./requirements.txt"
  "./test-requirements.txt"
  "./setup.py"
  "./.DS_Store"
  "./Makefile"
  "./poetry.lock"
  "./docker-compose.yml"
  "./.pre-commit-config.yaml"
  "./README.md"
)

EXCLUDE_EXTENSIONS=(
  "json"
  "iml"
  "keep"
  "gitignore"
)

# Process all files in the target directory
find . -type f | while read -r file; do
  if [ -f "$file" ]; then
    # Extract the extension
    ext="${file##*.}"

    # Check if we need to exclude this extension
    extension_excluded=false
    for exclude_ext in "${EXCLUDE_EXTENSIONS[@]}"; do
      if [ "$ext" = "$exclude_ext" ]; then
        extension_excluded=true
      fi
    done

    # Skip this file if its extension is excluded
    if [ "$extension_excluded" = true ]; then
      echo "${file}: skipped"
      continue
    else
      # Check if file should be excluded
      exclude=false
      for exclude_path in "${EXCLUDE_PATHS[@]}"; do
        if [[ "$file" == "$exclude_path"* ]]; then
          exclude=true
        fi
      done
      for exclude_path in "${KEEP_TESTS[@]}"; do
        if [[ "$file" == "$exclude_path"* ]]; then
          exclude=true
        fi
      done
      if [ "$exclude" = false ]; then
        # Construct the header file path
        header_file="${SCRIPT_DIR}/header-${ext}.txt"

        # Only process if the license file exists
        if [ -f "$header_file" ]; then
          echo "${file}: updated"
          prepend_header "$file" "$header_file"
        else
          echo "No header compatible with file ${file}"
          exit 2
        fi
      fi
    fi
  fi
done

echo "License fix complete"


DELETE_PATHS=(
  "test/__init__.py"
)

for path in "${DELETE_PATHS[@]}"; do
  rm -f "$path"
done

echo "Deletion complete"

echo "Regeneration complete"

popd > /dev/null
