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

readonly max_chars="${COPILOT_INSTRUCTIONS_MAX_CHARS:-3500}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

mapfile -t instruction_files < <(
  {
    [[ -f .github/copilot-instructions.md ]] && printf '%s\n' .github/copilot-instructions.md
    find .github/instructions -type f -name '*.instructions.md' 2>/dev/null || true
  } | sort
)

if [[ "${#instruction_files[@]}" -eq 0 ]]; then
  echo "No Copilot instruction files found."
  exit 0
fi

failed=0

for file in "${instruction_files[@]}"; do
  chars="$(wc -m < "$file" | tr -d '[:space:]')"
  remaining=$((max_chars - chars))

  if ((chars > max_chars)); then
    echo "ERROR: $file has $chars characters, exceeding limit $max_chars."
    failed=1
  else
    echo "$file: $chars characters ($remaining remaining)"
  fi

  if [[ "$file" == .github/instructions/*.instructions.md ]]; then
    if [[ "$(sed -n '1p' "$file")" != "---" ]] ||
      ! sed -n '2,/^---$/p' "$file" | grep -q '^applyTo:'; then
      echo "ERROR: $file must start with YAML frontmatter containing applyTo."
      failed=1
    fi
  fi
done

exit "$failed"
