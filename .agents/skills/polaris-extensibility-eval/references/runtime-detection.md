<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# Runtime capability detection

Probe the workspace once at run start. Cache results in
`.meta-eval/<run-id>/runtime.json`. Every step that has a "with X /
without X" branch reads from this map.

## Capabilities

| Capability | Probe | Used for |
|---|---|---|
| `has_claude_cli` | `command -v claude` | Out-of-process worker dispatch (preferred) |
| `has_codex_cli` | `command -v codex` | Cross-CLI multi-config matrix |
| `has_cursor_cli` | `command -v cursor-agent` | Cross-CLI matrix |
| `gradle_works` | `./gradlew --version` exits 0 | Verifier execution |
| `git_root` | `git rev-parse --show-toplevel` | All path resolution |
| `is_polaris_repo` | root contains `AGENTS.md` AND `polaris-core/` AND `gradle/projects.main.properties` | Refuse run otherwise |
| `available_models` | If `claude --version` works, default `[claude-opus-4-7]`; merge user-supplied list | Per-cell model dispatch |
| `default_dispatch_mode` | `has_claude_cli` ? `claude_cli_scrubbed_env` : `inprocess_agent_with_leak_risk` | Step 3b decision |

## Capability map → behavior

### Worker dispatch (Step 3b/4)

Preference order:

1. **`has_claude_cli` (or codex/cursor) AND skill/rule change**:
   `scripts/run_arm.sh` invokes the CLI in a fresh subprocess with
   `env -i HOME=/tmp/sb_<runid> PATH=/usr/bin:/bin
   ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY"` (or equivalent for the
   chosen agent CLI). Skill registry rebuilds from
   `$WORKTREE/.agents/` and `$WORKTREE/.claude/` only.

   *Snippet handling:* the line above is illustrative. The actual
   `scripts/run_arm.sh` builds the `env -i` argv internally via
   bash indirect expansion (`${!v}`) so secret values never appear
   in any logged string. Do **not** paste the resolved form (with
   `$ANTHROPIC_API_KEY` expanded) into a shell, CI yaml, Make
   recipe, or anything that may log argv or echo commands — argv is
   visible to other users via `ps auxewww` and any wrapper that
   prints `set -x`-style traces will leak the key. If you need to
   reproduce the dispatch outside this skill, source the credentials
   from a tool that injects them at exec time, not from a literal
   substitution.
2. **No CLI AND skill/rule change**: in-process Agent tool, mark
   `hygiene_violation_risk: high`. Run completes but its A/B
   conclusion on skill discoverability is degraded.
3. **Pure code/docs change** (no `.agents/`, `AGENTS.md`,
   `CLAUDE.md`, `.codex/`, `.cursor/` in the diff): in-process
   Agent tool is fine; the leak doesn't apply.

Record the chosen mode in `report.json.runtime.dispatch_mode`.

### Model spread

When `models: [...]` is non-empty in the task spec:

- Each model spawns its own cell. The base CLI dispatch line
  becomes `claude --model "$MODEL" -p ...`. For codex/cursor,
  consult the agent's docs for the equivalent flag.
- Exclude any model not in `available_models` (no point burning
  tokens on a 401).

### CLI spread

When `cli: [...]` is non-empty:

- Each CLI spawns its own cell.
- For each CLI, pick its native default model unless a model is
  explicitly set in the per-cell config.
- Skip CLIs whose `has_<cli>_cli` flag is false (do not error —
  just record `skipped: cli_unavailable`).

### Verifier examples

Polaris is gradle-only. The runtime detection writes:
`build_system: gradle` always. Verifier examples in
`references/task-spec.md` and the seed task YAMLs assume
`./gradlew :<module>:test --tests "<glob>"` or
`./gradlew compileAll`. There is no fallback to other build
systems; if `gradle_works` is false, refuse to run.

## Worked examples

Polaris repo with claude CLI + opus only:

```json
{
  "has_claude_cli": true,
  "has_codex_cli": false,
  "has_cursor_cli": false,
  "gradle_works": true,
  "is_polaris_repo": true,
  "available_models": ["claude-opus-4-7"],
  "default_dispatch_mode": "claude_cli_scrubbed_env"
}
```

Polaris repo with claude CLI + multi-model:

```json
{
  "has_claude_cli": true,
  "available_models": [
    "claude-haiku-4-5-20251001",
    "claude-sonnet-4-6",
    "claude-opus-4-7"
  ],
  "default_dispatch_mode": "claude_cli_scrubbed_env"
}
```

Bare repo with no CLI (in-process fallback):

```json
{
  "has_claude_cli": false,
  "default_dispatch_mode": "inprocess_agent_with_leak_risk"
}
```
