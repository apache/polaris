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
---
name: polaris-extensibility-eval
description: >
  Evaluate the impact of a proposed Polaris repo change on future agentic
  development by running fresh, context-free coding-agent subprocesses
  through concrete tasks against the BEFORE and AFTER of the change, then
  reporting A/B deltas in success rate, wall time, tokens, and reviewer-
  verifiability. Optionally varies model (haiku / sonnet / opus 4.5/4.6/4.7)
  and CLI (claude / codex / cursor) to surface impact that depends on
  agent capability. Use when weighing a non-functional change to AGENTS.md,
  the .agents/ skills, CLAUDE.md/CODEX.md/cursorrules, build files, or any
  refactor that changes the shape of an extension surface (authorizer,
  policy, federation, credentials, events, persistence). Triggers on:
  "A/B this rule", "does this AGENTS.md edit help agents", "measure
  agentic impact", "polaris extensibility eval", "is this refactor worth
  it for AI dev", "eval the skill".
argument-hint: "[BASE_REF..HEAD_REF or path being changed]"
---

# Polaris Extensibility-Eval Orchestrator

You are an orchestrator, not an evaluator. Your job is to spawn fresh,
context-free coding-agent subprocesses and measure how they perform
against a concrete task bank *before* and *after* a change. You must
NOT evaluate tasks yourself — your context window is polluted.

## When this skill applies

- **Primary**: non-functional changes that claim to help agents but
  have no verifiable success metric today — AGENTS.md edits, new
  `.agents/skills/*`, CLAUDE.md / Codex / Cursor rule additions,
  refactors of an extension surface (authorizer SPI, policy registry,
  federation factory, credential vendor, event listener, persistence
  SPI).
- **Secondary**: functional code changes where the reviewer also
  wants a maintainability / agentic-extensibility read.

If the change has zero plausible impact on agent behavior (typo in a
log string, dependency bump with no API change), say so and stop.
Don't burn agent budget on noise.

## Platform requirement

Requires Claude Code, `git`, and `gradle` (the repo's `./gradlew`
wrapper). Beyond that, the skill detects what's available — see
`references/runtime-detection.md`. With at least one of `claude`,
`codex`, or `cursor` CLI installed, runs fully out-of-process for
maximum context isolation. Without any CLI, falls back to the
in-process Agent tool and flags `hygiene_violation_risk: high` for
skill/rule changes.

## Inputs you need from the user

Before running, confirm via AskUserQuestion if not supplied:

1. **The change under evaluation** — `BASE_REF..HEAD_REF` (e.g.
   `main..HEAD` or two SHAs), or a file path whose before/after
   state you'll resolve to commits.
2. **Task source** — `seed` (filtered seed bank), `targeted` (spawn
   a generator), or `both`.
3. **Configuration grid** — number of (model × cli) cells per arm.
   Default: 1 model (opus-4-7), 1 CLI (claude). The user may
   request a richer grid like
   `models=[haiku-4-5, sonnet-4-6, opus-4-7]` to test model-spread.
4. **Budget cap.** Default per-cell: 200k tokens, 20 min, $2. Total
   guardrail: ask before exceeding ~$5.

Front-load the most important unresolved question.

## Workflow

### Step 0 — Detect runtime capabilities

Probe the workspace once. Write `.meta-eval/<run-id>/runtime.json`.
See `references/runtime-detection.md` for the full matrix. Minimum:

- `has_claude_cli` / `has_codex_cli` / `has_cursor_cli`:
  `command -v <bin>` succeeds.
- `gradle_works`: `./gradlew --version` succeeds.
- `available_models`: parse `claude --version` plus user-supplied
  list. Default `[claude-opus-4-7]`.
- `git_root`: `git rev-parse --show-toplevel`.
- `is_polaris_repo`: heuristic — root contains `AGENTS.md` AND
  `polaris-core/` AND `gradle/projects.main.properties`.

Refuse to run if `is_polaris_repo` is false. This skill is scoped
intentionally to the Polaris codebase.

### Step 1 — Resolve refs

```bash
BEFORE_REF=$(git rev-parse "$1^{commit}")
AFTER_REF=$(git rev-parse "$2^{commit}")
git diff --stat $BEFORE_REF..$AFTER_REF > .meta-eval/$RUN_ID/diff.stat
```

Verify both commits exist; warn if working tree is dirty.

### Step 2 — Select tasks

For each `tasks/seed/*.yaml`:

- Read `applies_when` (paths globs / keywords / `"always"`). If the
  diff matches, the task applies.
- If `targeted` mode is also requested, spawn ONE generator subagent
  with the diff (and *not* the rubric) — see
  `references/task-spec.md` "Targeted task generation".
- Display the candidate task set + projected cell count + projected
  cost; ask the user to confirm before running.

If zero seed tasks apply and `targeted` produces nothing
plausible, STOP. The change probably doesn't need this skill.

### Step 3 — Provision worktrees

Per arm:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%Sz)"
WORKSPACE=".meta-eval/$RUN_ID"
mkdir -p "$WORKSPACE"
git worktree add --detach "$WORKSPACE/wt-before" "$BEFORE_REF"
git worktree add --detach "$WORKSPACE/wt-after"  "$AFTER_REF"
```

### Step 3b — Decide dispatch mode

If any changed path is under `.agents/skills/`, `AGENTS.md`,
`CLAUDE.md`, `.codex/`, `.cursor/`, or any agent-config file, the
parent session's auto-injected skill / rule reminders WILL leak the
new content into BEFORE workers running in-process. Confirmed in an
earlier iteration of this harness on a different codebase.

Decision:

- **Skill/rule change AND `has_claude_cli`** (or codex/cursor) →
  use `scripts/run_arm.sh` which spawns a fresh subprocess with
  scrubbed env (`env -i HOME=$RUN_ID_SANDBOX PATH=...
  ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY ...`). The subprocess builds
  its skill registry from `$WORKTREE/.agents/skills/` (or
  `.claude/skills/` for compatibility) only.
- **Skill/rule change AND no CLI** → in-process Agent tool, flag
  `hygiene_violation_risk: high`. Be honest in the report.
- **Pure code/docs change** (none of the above paths) → in-process
  Agent tool is fine.

Record the chosen mode in `report.json.runtime.dispatch_mode`.

### Step 4 — Run cells

A "cell" is the tuple `(task, arm, model, cli)`. The grid is the
cartesian product of the selected tasks × {before, after} × the
user's model list × the user's CLI list. Default grid: 1 task × 2
arms × 1 model × 1 CLI = 2 cells.

Per cell:

1. Pre-warm: in the relevant worktree, run `./gradlew --version`
   once to populate the gradle cache. Skip per-cell if cache
   already warm.
2. Render the worker prompt = hygiene preamble (from
   `references/context-hygiene.md`) + (optionally
   `references/polaris-context.md` if the task sets
   `loads_polaris_context: true`) + the task's `worker_prompt`.
3. Dispatch via the chosen mode. Capture stdout, stderr, exit code,
   wall time, usage (input/output tokens, cost).
4. Run the task's `verifier` command from inside the worktree.
   Verdict = exit code 0 → PASS, else FAIL. Save the last 40 lines
   of verifier output.
5. Snapshot `git diff` inside the worktree for reviewer artifacts.
6. Run post-cell hygiene check: grep the agent's self-report for
   tokens that appear in the change diff but not in the
   worktree's pre-existing files. A hit on a BEFORE arm = leak.

Cells with `shared_resource: workspace_env` run serially. Cells
with no shared resource may run in parallel up to 2× per host
(gradle daemon contention dominates beyond that).

### Step 5 — Report A/B deltas

Write `.meta-eval/<run-id>/report.{md,json}`. The MD report has
three tables:

```
## Per-task summary
| Task                | Before(opus) | After(opus) | Δ time | Δ tokens | Verdict |
|---------------------|--------------|-------------|--------|----------|---------|
| T-priv-add          | FAIL 480s    | PASS 320s   | -33%   | -22%     | IMPROV  |

## Per-model spread (when grid > 1 model)
| Task        | Model   | Before | After | Δ tokens | Notes      |
|-------------|---------|--------|-------|----------|------------|
| T-priv-add  | haiku   | FAIL   | PASS  | -22%     | improved  |
| T-priv-add  | sonnet  | PASS   | PASS  | -8%      | neutral   |
| T-priv-add  | opus    | PASS   | PASS  | -3%      | neutral   |

## Hygiene
violations: 0 / 0  (BEFORE arms did not see leaked AFTER content)
dispatch_mode: claude_cli_scrubbed_env
```

Surfacing rules:
- **Regression** (PASS → FAIL): block acceptance unless justified.
- **Improvement** (FAIL → PASS): quote verifier output + diff.
- **Soft-improvement / soft-regression**: PASS→PASS or FAIL→FAIL
  with > 15% change in tokens / time / tool-uses. For non-functional
  changes whose whole point is reducing friction, the cost delta IS
  the signal. Do NOT collapse into "Neutral".
- **Hygiene violation**: any post-cell grep hit → flag run as
  inconclusive on the affected metric.

### Step 6 — Persist + cleanup

`git worktree remove --force "$WT"` for each arm (in a `trap EXIT`
or equivalent — orphan worktrees accumulate fast). Keep
`.meta-eval/<run-id>/` for artifacts. Append a one-line summary to
`.meta-eval/index.jsonl`.

Do NOT auto-apply or auto-revert the change. The skill is advisory.

## Context hygiene

See `references/context-hygiene.md`. Key rules:

- The hygiene preamble is non-negotiable — every worker prompt must
  begin with it verbatim.
- Skill-registry / rule-registry leak is a known harness limit on
  in-process Agent dispatch. Out-of-process CLI dispatch with
  scrubbed env is the only full mitigation.
- Post-run check: grep each arm's self-report for tokens that
  appear in the change diff but not in the worktree's pre-existing
  files.

## Metrics schema

See `references/metrics.md`. Polaris-specific extensions vs. the
generic skill:

- `cell.model` and `cell.cli` are first-class.
- `arm.gradle_warm_cache_ms` is recorded so per-cell wall time can
  be normalized.
- `task.loads_polaris_context: bool` is preserved for analysis —
  tasks that don't load it are the truer discoverability probe.

## Task spec format

See `references/task-spec.md`. New fields vs. the generic skill:

```yaml
loads_polaris_context: false   # if true, prepend references/polaris-context.md
                               # to the worker prompt
models: []                     # default: orchestrator's primary
cli:    []                     # default: orchestrator's primary
```

## Seed task bank (current)

- `T-priv-add.yaml` — add a new authorization privilege through the
  enum + semantics map + tests. Probes the most-contested area
  (authorizer SPI / RBAC).
- `T-policy-add.yaml` — add a new predefined policy type by
  mirroring an existing JSON-schema folder. Probes
  registry-discoverability.
- `T-event-add.yaml` — add a new event class + listener wiring +
  unit test. Probes the recently-flattened events hierarchy.
- `T-fed-catalog-stub.yaml` — stub a new federation extension
  module that compiles. Probes build-system / Gradle-projects
  discoverability.

Tasks were chosen by mining PR / mailing-list history for areas
with the most refactor friction. See
`references/polaris-context.md` for the context loaded into workers
that opt in.

## Bounded recursion

This skill CAN evaluate a change to itself — that's exactly the
non-functional case it was built for. But:

- Worker tasks must not invoke this skill recursively. Reject any
  generated task that would.
- Reviewer-verifiability of the skill's own output bottoms out at
  "report renders, JSON parses, worktrees clean up".

## Out of scope

Always:
- Auto-applying improvements — advisory only.
- Refactor-quality code review — use `review` / `simplify` if
  available.
- Security review — use `security-review` if available.
- Long-running integration tests that need real S3/BigQuery/HMS
  endpoints (the fed-catalog-stub task is compile-only).

## Gotchas

- **Gradle daemon contention.** Don't run more than 2 cells in
  parallel on one host; gradle wedges.
- **Worktree cleanup.** Always `git worktree remove --force` in a
  `trap EXIT`. Polaris's worktree is large (~250 MB checked out);
  orphan worktrees fill the disk fast.
- **Token burn.** A bad task prompt can cause a worker to thrash
  for 50k+ tokens. `time_budget_min` is a hard cap; if the
  subprocess exceeds it, kill and record `verdict: ERROR,
  reason: budget_exceeded`.
- **Hygiene false negative.** An overly-eager subagent may `find /`
  and accidentally read user-global agent rules. The post-run
  grep catches this on text that appears in the diff but not in
  the worktree.
- **Noise.** Default `k=1` per cell. Set `k=2` (or `k=3`) for
  any task whose verdict is on the boundary.

## Examples

### Example 1 — Evaluating an AGENTS.md edit

User: "I added a 'Adding a new authorization privilege' section to
AGENTS.md. Does it actually help agents?"

You:
1. `BEFORE_REF = git show HEAD~1`, `AFTER_REF = HEAD`.
2. Filter seed tasks by `applies_when`. T-priv-add applies (paths
   include `polaris-core/.../auth/**`).
3. Default grid: 1 task × 2 arms × 1 model × 1 CLI = 2 cells.
4. Run, render report. Expect either improvement (PASS→PASS with
   token drop > 15% = soft-improvement) or no-op.

### Example 2 — Evaluating a refactor that flattens an SPI

User: "I unified `ExternalCatalogFactory` and
`FederatedCatalogFactory` — does adding a new federation backend
get easier?"

You:
1. Use T-fed-catalog-stub.
2. Suggest grid: 1 task × 2 arms × 2 models (sonnet, opus) — to
   test whether the simpler API helps small models more.
3. Cost projection: ~4 cells × ~$0.50 = ~$2; OK without prompting.

### Example 3 — Rejecting a no-op invocation

User: "I bumped Quarkus from 3.35.2 to 3.35.3. Meta-eval it?"

You: "No agentic-development surface changed. Skipping; this is a
dependency tracking concern, not an extensibility concern. If you
want a smoke check anyway, rerun with `--force`."
