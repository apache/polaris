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

# redocly drops the leading comment block when it bundles, so the ASF header is taken from the
# source spec and put back afterwards. See spec/README.md.
readonly source_spec="spec/polaris-catalog-service.yaml"
readonly bundle="spec/generated/bundled-polaris-catalog-service.yaml"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -n "${REDOCLY:-}" ]]; then
  read -r -a redocly <<< "$REDOCLY"
elif command -v redocly >/dev/null 2>&1; then
  redocly=(redocly)
elif command -v npx >/dev/null 2>&1; then
  redocly=(npx -y @redocly/cli)
else
  echo "ERROR: neither redocly nor npx found. Install it with 'npm install -g @redocly/cli', or point REDOCLY at a runnable command." >&2
  exit 1
fi
echo "Bundling with: ${redocly[*]}"

header="$(mktemp)"
trap 'rm -f "$header"' EXIT
awk '/^#/ || /^[[:space:]]*$/ { print; next } { exit }' "$source_spec" > "$header"

header_lines="$(wc -l < "$header")"
if [[ "$header_lines" -ne 19 ]] || ! grep -q "Licensed to the Apache Software Foundation" "$header"; then
  echo "ERROR: expected the 19 line ASF header at the top of $source_spec, found $header_lines lines." >&2
  exit 1
fi

"${redocly[@]}" bundle "$source_spec" -o "$bundle"

# Fires only if the bundler ever starts preserving comments; keeps a rerun from stacking headers.
if [[ "$(head -n "$header_lines" "$bundle")" == "$(cat "$header")" ]]; then
  echo "$bundle already carries the license header."
  exit 0
fi

cat "$header" "$bundle" > "$bundle.tmp"
mv "$bundle.tmp" "$bundle"
echo "Regenerated $bundle with the license header restored."
