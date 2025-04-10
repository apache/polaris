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

pushd "$(dirname "$0")/.." > /dev/null
TARGET_DIR="$(dirname "$0")/../python"

#############################
# Regenerate Python clients #
#############################

echo "Regenerating python from the spec"

# TODO skip-validate-spec is needed because the upstream Iceberg spec seems invalid. e.g.:
#   [main] ERROR o.o.codegen.DefaultCodegen - Required var sort-order-id not in properties

docker run --rm \
  -v "$(dirname "$0")/../..:/local" \
  openapitools/openapi-generator-cli generate \
  -i /local/spec/polaris-management-service.yml \
  -g python \
  -o /local/client/python \
  --additional-properties=packageName=polaris.management \
  --additional-properties=apiNamePrefix=polaris > /dev/null 

docker run --rm \
  -v "$(dirname "$0")/../..:/local" \
  openapitools/openapi-generator-cli generate \
  -i /local/spec/polaris-catalog-service.yaml \
  -g python \
  -o /local/client/python \
  --additional-properties=packageName=polaris.catalog \
  --additional-properties=apiNameSuffix="" \
  --skip-validate-spec > /dev/null

docker run --rm \
  -v "$(dirname "$0")/../..:/local" \
  openapitools/openapi-generator-cli generate \
  -i /local/spec/iceberg-rest-catalog-open-api.yaml \
  -g python \
  -o /local/client/python \
  --additional-properties=packageName=polaris.catalog \
  --additional-properties=apiNameSuffix="" \
  --additional-properties=apiNamePrefix=Iceberg \
  --skip-validate-spec > /dev/null

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

# List of paths to exclude (from .../client)
EXCLUDE_PATHS=(
  "python/.gitignore"
  "python/.openapi-generator/"
  "python/.openapi-generator-ignore"
  "python/pyproject.toml"
)

EXCLUDE_EXTENSIONS=(
  "json"
)

# Process all files in the target directory
git ls-files "${TARGET_DIR}" | while read -r file; do
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
      # Construct the header file path
      header_file="$(dirname "$0")/header-${ext}.txt"
    
      # Only process if the license file exists
      if [ -f "$header_file" ]; then
        echo "${file}: updated"
        prepend_header "$file" "$header_file"
      else
        # Check if file should be excluded
        exclude=false
        for exclude_path in "${EXCLUDE_PATHS[@]}"; do
          if [[ "$file" == "$exclude_path"* ]]; then
            exclude=true
          fi
        done
      
        if [ "$exclude" = false ]; then
          echo "No header compatible with file ${file}"
          exit 2
        fi
      fi
    fi
  fi
done

echo "Regeneration complete"

popd > /dev/null

