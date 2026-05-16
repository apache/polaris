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
# detect.sh — probe runtime capabilities, write runtime.json.
# Usage: detect.sh <repo_root> <output_path>
set -euo pipefail

REPO_ROOT="${1:-$PWD}"
OUTPUT="${2:-/dev/stdout}"

probe_cmd() { command -v "$1" >/dev/null 2>&1 && echo true || echo false; }

cd "$REPO_ROOT"

HAS_CLAUDE=$(probe_cmd claude)
HAS_CODEX=$(probe_cmd codex)
HAS_CURSOR=$(probe_cmd cursor-agent)
HAS_JQ=$(probe_cmd jq)
HAS_GIT=$(probe_cmd git)

GRADLE_WORKS=false
if [ -x "./gradlew" ] && ./gradlew --version >/dev/null 2>&1; then
  GRADLE_WORKS=true
fi

GIT_ROOT=""
if [ "$HAS_GIT" = "true" ]; then
  GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "")
fi

IS_POLARIS=false
if [ -f "$GIT_ROOT/AGENTS.md" ] \
   && [ -d "$GIT_ROOT/polaris-core" ] \
   && [ -f "$GIT_ROOT/gradle/projects.main.properties" ]; then
  IS_POLARIS=true
fi

DEFAULT_MODE="inprocess_agent_with_leak_risk"
if [ "$HAS_CLAUDE" = "true" ] || [ "$HAS_CODEX" = "true" ] || [ "$HAS_CURSOR" = "true" ]; then
  DEFAULT_MODE="cli_scrubbed_env"
fi

cat > "$OUTPUT" <<EOF
{
  "has_claude_cli": $HAS_CLAUDE,
  "has_codex_cli":  $HAS_CODEX,
  "has_cursor_cli": $HAS_CURSOR,
  "has_jq":         $HAS_JQ,
  "gradle_works":   $GRADLE_WORKS,
  "git_root":       "$GIT_ROOT",
  "is_polaris_repo": $IS_POLARIS,
  "default_dispatch_mode": "$DEFAULT_MODE",
  "available_models": ["claude-opus-4-7"]
}
EOF
