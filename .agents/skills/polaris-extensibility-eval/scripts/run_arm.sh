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
# run_arm.sh — dispatch a single (task, arm, model, cli) cell with
# scrubbed env so the agent CLI rebuilds its skill registry from
# the worktree only. Supports Anthropic API direct, AWS Bedrock,
# and Google Vertex auth backends.
#
# Usage:
#   run_arm.sh <worktree_path> <prompt_file> <out_dir> \
#              <cli> <model> <task_id> <arm>
#
# Outputs to <out_dir>/{stdout.txt, stderr.txt, exit_code.txt,
#                       timing.json, usage.json, worker.diff}.

set -euo pipefail

WORKTREE="$(cd "$1" && pwd)"
# Absolutize prompt file and output dir BEFORE cd'ing into the
# worktree below, otherwise relative paths break.
PROMPT_FILE="$(realpath "$2")"
mkdir -p "$3"
OUT_DIR="$(cd "$3" && pwd)"
CLI="${4:-claude}"
MODEL="${5:-claude-opus-4-7}"
TASK_ID="${6:-unknown}"
ARM="${7:-unknown}"
SANDBOX_HOME=$(mktemp -d -t pol_eval_${TASK_ID}_${ARM}_XXXXXX)
trap 'rm -rf "$SANDBOX_HOME"' EXIT

START_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
START_S=$(date +%s)

# Build the env passthrough list. Pass through whichever auth set
# the caller has populated; agents will fail fast if none works.
build_env_args() {
  local args=(
    "HOME=$SANDBOX_HOME"
    "PATH=${PATH}"
    "TERM=dumb"
    "LANG=${LANG:-C.UTF-8}"
    # Java env for ./gradlew; Polaris needs Java 21 even though the
    # wrapper itself runs on 17.
    "JAVA_HOME=${JAVA_HOME:-}"
    # Block any inherited Claude Code session signal.
    "CLAUDECODE="
  )
  # Anthropic API direct.
  for v in ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN ANTHROPIC_BASE_URL ANTHROPIC_API_BASE; do
    [ -n "${!v:-}" ] && args+=("$v=${!v}")
  done
  # AWS Bedrock.
  for v in CLAUDE_CODE_USE_BEDROCK AWS_REGION AWS_DEFAULT_REGION \
           AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN \
           AWS_ROLE_ARN AWS_WEB_IDENTITY_TOKEN_FILE \
           AWS_STS_REGIONAL_ENDPOINTS AWS_PROFILE; do
    [ -n "${!v:-}" ] && args+=("$v=${!v}")
  done
  # Google Vertex.
  for v in CLAUDE_CODE_USE_VERTEX ANTHROPIC_VERTEX_PROJECT_ID \
           GOOGLE_APPLICATION_CREDENTIALS CLOUD_ML_REGION; do
    [ -n "${!v:-}" ] && args+=("$v=${!v}")
  done
  # OpenAI for codex CLI.
  for v in OPENAI_API_KEY OPENAI_BASE_URL; do
    [ -n "${!v:-}" ] && args+=("$v=${!v}")
  done
  printf '%s\n' "${args[@]}"
}

ENV_ARGS=()
while IFS= read -r line; do
  ENV_ARGS+=("$line")
done < <(build_env_args)

cd "$WORKTREE"

case "$CLI" in
  claude)
    # Use stream-json to capture per-iteration usage. Allow edits
    # but disallow Bash for tasks that don't need it; skill-level
    # tasks here DO need Bash (for ./gradlew), so allow it.
    # acceptEdits (silent edit/write) + explicit Bash allowlist:
    # `auto` mode classifier can return "ask" in non-interactive -p
    # mode and stall the run; `acceptEdits` alone blocks Bash. Combined
    # with --allowedTools we get the minimum-privilege set workers
    # need to verify with ./gradlew.
    env -i "${ENV_ARGS[@]}" \
      claude -p "$(cat "$PROMPT_FILE")" \
        --model "$MODEL" \
        --permission-mode acceptEdits \
        --allowedTools "Bash Edit Write Read Glob Grep" \
        --output-format json \
        > "$OUT_DIR/stdout.txt" 2> "$OUT_DIR/stderr.txt" \
      < /dev/null \
      || true
    EXIT_CODE=${PIPESTATUS[0]:-1}
    ;;
  codex)
    env -i "${ENV_ARGS[@]}" \
      codex exec \
        --cd "$WORKTREE" \
        -m "$MODEL" \
        -s workspace-write \
        --skip-git-repo-check \
        "$(cat "$PROMPT_FILE")" \
        > "$OUT_DIR/stdout.txt" 2> "$OUT_DIR/stderr.txt" \
      < /dev/null \
      || true
    EXIT_CODE=${PIPESTATUS[0]:-1}
    ;;
  cursor)
    env -i "${ENV_ARGS[@]}" \
      cursor-agent \
        --model "$MODEL" \
        --force \
        --sandbox enabled \
        "$(cat "$PROMPT_FILE")" \
        > "$OUT_DIR/stdout.txt" 2> "$OUT_DIR/stderr.txt" \
      < /dev/null \
      || true
    EXIT_CODE=${PIPESTATUS[0]:-1}
    ;;
  *)
    echo "unsupported cli: $CLI" >&2
    exit 2
    ;;
esac

END_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
END_S=$(date +%s)
DURATION=$((END_S - START_S))

echo "$EXIT_CODE" > "$OUT_DIR/exit_code.txt"

cat > "$OUT_DIR/timing.json" <<EOF
{
  "start_iso": "$START_ISO",
  "end_iso":   "$END_ISO",
  "duration_seconds": $DURATION
}
EOF

# Usage parsing: Claude --output-format json embeds the data in the
# top-level result. For other CLIs this is best-effort.
if [ "$CLI" = "claude" ] && command -v jq >/dev/null 2>&1; then
  jq -r '{
    cost_usd: (.total_cost_usd // null),
    input_tokens:  (.usage.input_tokens  // null),
    output_tokens: (.usage.output_tokens // null),
    cache_read_input_tokens: (.usage.cache_read_input_tokens // null),
    cache_creation_input_tokens: (.usage.cache_creation_input_tokens // null),
    duration_api_ms: (.duration_api_ms // null),
    num_turns: (.num_turns // null),
    stop_reason: (.stop_reason // null),
    model_used: (.model // null)
  }' < "$OUT_DIR/stdout.txt" > "$OUT_DIR/usage.json" 2>/dev/null \
    || echo '{}' > "$OUT_DIR/usage.json"
else
  echo '{}' > "$OUT_DIR/usage.json"
fi

# Snapshot worktree diff for reviewer artifacts.
( cd "$WORKTREE" && git diff > "$OUT_DIR/worker.diff" 2>/dev/null || true )
( cd "$WORKTREE" && git diff --stat > "$OUT_DIR/worker.diffstat" 2>/dev/null || true )

exit "$EXIT_CODE"
