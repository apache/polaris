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
# Task spec format

Each task in the seed bank is a single YAML file under
`tasks/seed/`. Generated tasks live under
`.meta-eval/<run-id>/generated-tasks/` and follow the same schema.

## Required fields

```yaml
id: T-<namespace>-<shortname>
title: "Human-readable short title"
description: >
  1-3 sentences. What this task tests and why it discriminates
  between BEFORE and AFTER for some class of change.

applies_when:
  - paths: ["AGENTS.md", "polaris-core/src/main/java/.../auth/**"]
  - keywords: ["privilege", "authorizer", "RBAC"]
  - changed_files_gt: 0
  - "always"

shared_resource: null    # null | "workspace_env" (gradle daemon) | "sut"
time_budget_min: 20
k: 1                     # repetitions per (arm, model, cli) cell

loads_polaris_context: false
# When true, references/polaris-context.md is prepended to the
# worker prompt. Use false when the task is probing
# discoverability (e.g. "does AGENTS.md alone teach the agent how
# to add a privilege?").

models: []               # default: orchestrator's primary model
cli:    []               # default: orchestrator's primary CLI

worker_prompt: |
  <full self-contained task prompt — the hygiene preamble from
   references/context-hygiene.md is prepended automatically>

verifier:
  kind: deterministic    # deterministic | narrow-deterministic | llm-judge
  command: "./gradlew :polaris-core:test --tests '<glob>'"
  timeout_seconds: 600
  success_on:
    exit_code: 0
```

## Optional fields

```yaml
baseline_verdict:        # what's expected before tooling helps
  before: FAIL
  after:  PASS

cleanup_command: "rm -rf /tmp/scratch_<task>"
tags: ["docs", "auth", "extensibility-friction"]
```

## Quality rules

1. **Verifier is deterministic.** Prefer `./gradlew :<module>:test
   --tests "<glob>"` or `./gradlew compileAll`. Use `grep -q` only
   for content-depth checks that exit 0 / 1.
2. **Self-contained prompt.** The worker should not need to ask
   clarifying questions. Any ambiguity IS the task.
3. **Time-bounded.** ≤ 20 minutes per cell at the 95th percentile.
   Split larger tasks.
4. **Fresh-startable.** A worker with `loads_polaris_context:
   false` should be able to start by reading repo files, not by
   asking for context.
5. **Change-sensitive.** If the change under eval doesn't
   plausibly affect this task, it shouldn't be in `applies_when`.
6. **No cheating vectors.** If the verifier is `grep "FOO"`, the
   prompt must not mention `"FOO"`.
7. **Reports a diff, not just a verdict.** The verifier should
   emit a trail (test log, compile output, diff) a human reviewer
   can inspect. Verdict alone is low reviewer-verifiability.

## Anti-patterns

- "Read a file and summarize it" — passes on both arms; doesn't
  exercise behavior.
- "Implement a real catalog backend end-to-end" — too large; will
  time out and dominate token budget.
- "Is this refactor good?" — LLM-judge only, no verifier.
- Tasks that modify files outside the worktree.

## Guarding against the registry leak

When the change under eval adds or modifies AGENTS.md or
`.agents/skills/**`, the BEFORE worker may see the new content's
description via the registry leak (see context-hygiene.md). Two
defenses at the task level:

1. **Prefer content-depth verifiers** that require reading the
   AGENTS.md body or SKILL.md body, not just "the registry says X
   exists". E.g. verify the worker quotes a phrase from a section
   that wasn't summarized in the registry.
2. **Set `k: 2`** for any task whose verdict will be on the
   boundary. Single-run deltas under leak conditions are
   unreliable.

## Targeted task generation

When the user requests `targeted` mode, spawn ONE generator
subagent (NOT worktree-isolated; just proposes tasks). Give it the
diff + this YAML schema + `references/polaris-context.md`. Do NOT
give it `references/metrics.md` or any rubric — the generator must
not know the grading scheme.

Each generated task must:
1. Have a clear goal in one sentence.
2. Be self-contained.
3. Have a deterministic verifier (gradle / grep).
4. Specify `shared_resource` if relevant.
5. Estimate < 20 min wall time.

The orchestrator displays generated tasks for human approval before
running.

## Reporting soft-improvements

A PASS→PASS task with > 15% drop in tokens / wall time / tool-uses
is a *soft improvement*. Do not collapse it into "neutral" — for
non-functional changes whose whole point is reducing agent
friction, the cost delta IS the signal. Surface as
`category: soft_improvement_by_cost` in report.json and highlight
in report.md.
