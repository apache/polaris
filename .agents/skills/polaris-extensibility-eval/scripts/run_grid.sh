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
# run_grid.sh — drive a comprehensive A/B grid over (task, arm, model).
#
# Usage:
#   run_grid.sh <run_dir> <manifest>
#
# Manifest format (TSV, one cell per line):
#   <cell_id>\t<task_yaml>\t<worktree_path>\t<model>\t<cli>
#
# All cells are dispatched serially because they share the workspace
# gradle daemon. Each cell:
#   1. Renders prompt = hygiene preamble + task worker_prompt
#   2. Calls run_arm.sh for fresh-process dispatch in scrubbed env
#   3. Runs the task verifier inside the worktree
#   4. Records verdict + cleans the worktree before next cell

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${1:?usage: run_grid.sh <run_dir> <manifest>}"
MANIFEST="${2:?usage: run_grid.sh <run_dir> <manifest>}"

mkdir -p "$RUN_DIR/cells"

# Use Python helpers to extract YAML block scalars cleanly.
extract_field() {
  local file="$1" field="$2"
  python3 - "$file" "$field" <<'PY'
import sys, re
path, field = sys.argv[1], sys.argv[2]
lines = open(path).read().splitlines()
out = []
in_block = False
block_indent = None
for line in lines:
    if not in_block:
        m = re.match(rf'^{field}:\s*[|>]?\s*$', line)
        if m:
            in_block = True
            continue
    else:
        # Empty line inside a block stays in the block.
        if line.strip() == '':
            out.append('')
            continue
        # First non-empty line establishes the indent.
        ind = len(line) - len(line.lstrip(' '))
        if block_indent is None:
            if ind == 0:
                # Block ended (back to col 0 = next top-level field).
                break
            block_indent = ind
        if ind < block_indent:
            break
        out.append(line[block_indent:])
# Trim trailing empties
while out and out[-1] == '':
    out.pop()
print('\n'.join(out))
PY
}

extract_verifier() {
  local file="$1"
  python3 - "$file" <<'PY'
import sys, re
lines = open(sys.argv[1]).read().splitlines()
# Find verifier: block, then within it find command: block.
state = "scan"  # scan -> in_verifier -> in_command -> done
indent_v = None
indent_c = None
out = []
for line in lines:
    if state == "scan":
        if re.match(r'^verifier:\s*$', line):
            state = "in_verifier"
        continue
    if state == "in_verifier":
        if line.strip() == '':
            continue
        ind = len(line) - len(line.lstrip(' '))
        if ind == 0:
            break  # left the verifier block
        if indent_v is None:
            indent_v = ind
        if ind < indent_v:
            break
        m = re.match(r'^\s*command:\s*[|>]?\s*$', line)
        if m:
            state = "in_command"
            continue
        m2 = re.match(r'^\s*command:\s*"([^"]+)"\s*$', line)
        if m2:
            print(m2.group(1))
            sys.exit(0)
        continue
    if state == "in_command":
        if line.strip() == '':
            out.append('')
            continue
        ind = len(line) - len(line.lstrip(' '))
        if indent_c is None:
            if ind <= (indent_v or 0):
                break
            indent_c = ind
        if ind < indent_c:
            break
        out.append(line[indent_c:])
while out and out[-1] == '':
    out.pop()
print('\n'.join(out))
PY
}

HYGIENE_PREAMBLE='You are a fresh agent evaluating a Polaris coding task. Follow these
invariants without exception:

1. Treat the working directory ($WORKTREE) as your ONLY source of
   truth. Do NOT read files under:
     - $HOME/.claude/, $HOME/.codex/, $HOME/.cursor/,
       $HOME/.config/claude/, $HOME/.config/codex/,
       $HOME/.config/cursor/, $HOME/.copilot/
     - /etc/claude*, /etc/codex*, /etc/cursor*
     - Any file matching MEMORY.md anywhere outside $WORKTREE
   If you find yourself about to read one of those, stop and say
   "hygiene violation avoided".

2. Do NOT ask the user clarifying questions. Any ambiguity in the
   task is part of the task — resolve it using repo contents alone
   or state it as an unresolved gap and continue.

3. Do NOT consult any prior conversation. You have none.

4. Do NOT read or echo your process environment. Specifically:
   - Do not run env, printenv, set (without args), export -p,
     declare -p, compgen -e, or shell-builtins that dump env.
   - Do not read /proc/self/environ, /proc/$$/environ,
     /proc/*/environ, or any other path that exposes process env.
   - Do not include the value of any environment variable in your
     output, self-report, comments, file contents, or commit
     messages — not even ones that look benign (PATH, HOME, etc.).
   The harness passes auth credentials (ANTHROPIC_API_KEY, AWS_*,
   OPENAI_API_KEY, GOOGLE_APPLICATION_CREDENTIALS, …) to your CLI
   via process env so it can reach its inference endpoint. Reading
   those values back into your transcript would exfiltrate them. If
   a task seems to require an env-var value, treat that as an
   unresolved gap (rule 2) and stop.

5. You may and should: read AGENTS.md, .agents/skills/<any>/SKILL.md,
   CLAUDE.md, .codex/instructions.md, and any source/test files
   inside $WORKTREE. Run ./gradlew and git commands. Execute the
   tasks verifier.

6. When finished, print:
     ## Outcome
     <one-paragraph summary>
     ## Files changed
     <list>
     ## Verifier
     <command you ran and its exit code>
'

CELL_LOG="$RUN_DIR/grid.log"
echo "[grid] starting at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$CELL_LOG"

while IFS=$'\t' read -r CELL_ID TASK_YAML WORKTREE MODEL CLI _rest; do
  [ -z "$CELL_ID" ] && continue
  case "$CELL_ID" in \#*) continue ;; esac

  CELL_DIR="$RUN_DIR/cells/$CELL_ID"
  mkdir -p "$CELL_DIR"

  echo "[grid] cell=$CELL_ID  task=$(basename "$TASK_YAML")  model=$MODEL  cli=$CLI  wt=$(basename "$WORKTREE")" \
    | tee -a "$CELL_LOG"

  PROMPT_FILE="$CELL_DIR/prompt.txt"
  {
    echo "$HYGIENE_PREAMBLE"
    echo ""
    echo "Note: \$WORKTREE refers to '$WORKTREE' which IS your current"
    echo "working directory at task start."
    echo ""
    echo "--- THE TASK ---"
    echo ""
    extract_field "$TASK_YAML" worker_prompt
    echo ""
    echo "--- END TASK ---"
  } > "$PROMPT_FILE"

  CELL_START=$(date +%s)
  "$SCRIPT_DIR/run_arm.sh" \
    "$WORKTREE" "$PROMPT_FILE" "$CELL_DIR" \
    "$CLI" "$MODEL" \
    "$(basename "$TASK_YAML" .yaml)" "$CELL_ID" \
    || true
  CELL_END=$(date +%s)

  WORKER_EXIT=$(cat "$CELL_DIR/exit_code.txt" 2>/dev/null || echo 1)

  VERIFIER_FILE="$CELL_DIR/verifier.sh"
  {
    echo "#!/usr/bin/env bash"
    echo "set -e"
    # Polaris's settings.gradle.kts hard-rejects Java != 21. Auto-
    # discover a Java 21 install if JAVA_HOME points elsewhere.
    echo 'if [ -z "${JAVA_HOME:-}" ] || [ ! -x "$JAVA_HOME/bin/java" ] || ! "$JAVA_HOME/bin/java" -version 2>&1 | grep -qE "version \"21\\.|version 21\\."; then'
    echo '  for cand in /nix/store/*temurin*21* /nix/store/*-jdk-21* /usr/lib/jvm/*21* /opt/jdk21 /opt/java/openjdk; do'
    echo '    if [ -x "$cand/bin/java" ] && "$cand/bin/java" -version 2>&1 | grep -qE "version \"21\\.|version 21\\."; then'
    echo '      export JAVA_HOME="$cand"; break'
    echo '    fi'
    echo '  done'
    echo 'fi'
    echo 'export PATH="$JAVA_HOME/bin:$PATH"'
    echo "cd '$WORKTREE'"
    extract_verifier "$TASK_YAML"
  } > "$VERIFIER_FILE"
  chmod +x "$VERIFIER_FILE"

  bash "$VERIFIER_FILE" > "$CELL_DIR/verifier.stdout" 2> "$CELL_DIR/verifier.stderr" \
    && VERIFIER_EXIT=0 || VERIFIER_EXIT=$?
  echo "$VERIFIER_EXIT" > "$CELL_DIR/verifier_exit.txt"

  if [ "$VERIFIER_EXIT" -eq 0 ]; then
    echo "PASS" > "$CELL_DIR/verdict.txt"
  else
    echo "FAIL" > "$CELL_DIR/verdict.txt"
  fi

  WALL=$((CELL_END - CELL_START))
  COST=$(jq -r '.cost_usd // "null"' "$CELL_DIR/usage.json" 2>/dev/null || echo "null")
  TOK_IN=$(jq -r '.input_tokens // "null"' "$CELL_DIR/usage.json" 2>/dev/null || echo "null")
  TOK_OUT=$(jq -r '.output_tokens // "null"' "$CELL_DIR/usage.json" 2>/dev/null || echo "null")
  TURNS=$(jq -r '.num_turns // "null"' "$CELL_DIR/usage.json" 2>/dev/null || echo "null")

  echo "[grid] cell=$CELL_ID  verdict=$(cat $CELL_DIR/verdict.txt)  wall=${WALL}s  cost=\$$COST  in=$TOK_IN  out=$TOK_OUT  turns=$TURNS  worker_exit=$WORKER_EXIT" \
    | tee -a "$CELL_LOG"

  # Snapshot diff (post-verifier) so we can inspect FAILs even after
  # the next-cell cleanup. We already wrote worker.diff in run_arm.sh
  # but the verifier could have done its own work; capture again.
  ( cd "$WORKTREE" && git diff > "$CELL_DIR/post_verifier.diff" 2>/dev/null || true )

  # Reset worktree for the next cell.
  ( cd "$WORKTREE" && git reset --hard HEAD >/dev/null 2>&1 \
    && git clean -fdx >/dev/null 2>&1 ) || true

  # Stop gradle daemon to avoid memory pressure across cells.
  ( cd "$WORKTREE" && ./gradlew --stop >/dev/null 2>&1 ) || true
done < "$MANIFEST"

echo "[grid] all cells done at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$CELL_LOG"
