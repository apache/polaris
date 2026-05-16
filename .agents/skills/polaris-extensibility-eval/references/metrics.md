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
# Metrics schema

Every run emits one `report.json` with this shape.

## Run-level

```json
{
  "run_id": "20260514T140000z",
  "change": {
    "before_ref": "abc123",
    "after_ref":  "def456",
    "files_changed": ["AGENTS.md", ".agents/skills/foo/SKILL.md"],
    "additions": 47, "deletions": 3,
    "classification": "non-functional" | "functional" | "mixed"
  },
  "runtime": {
    "dispatch_mode": "claude_cli_scrubbed_env" | "inprocess_agent_with_leak_risk",
    "available_models": ["claude-opus-4-7"],
    "available_clis": ["claude"]
  },
  "tasks": [ ...task-level... ],
  "summary": {
    "n_cells": 2,
    "regressions": 0, "improvements": 0,
    "soft_improvements_by_cost": 1,
    "soft_regressions_by_cost": 0,
    "neutral": 1,
    "inconclusive": 0,
    "hygiene_violations": 0,
    "total_cost_usd": 0.42
  }
}
```

## Task-level

```json
{
  "task_id": "T-priv-add",
  "source": "seed",
  "loads_polaris_context": false,
  "shared_resource": null,
  "cells": [
    {
      "arm": "before", "model": "claude-opus-4-7", "cli": "claude",
      "verdict": "PASS", "verifier_exit_code": 0,
      "duration_seconds": 480,
      "input_tokens": 18000, "output_tokens": 6200,
      "cost_usd": 0.21,
      "tool_uses": 11,
      "files_changed_by_worker": ["polaris-core/.../auth/PolarisAuthorizableOperation.java", "..."],
      "hygiene_violations": [],
      "gradle_warm_cache_ms": 240
    },
    { "arm": "after", "model": "claude-opus-4-7", "cli": "claude", ... }
  ],
  "deltas_by_axis": {
    "before_vs_after_per_model": [
      { "model": "claude-opus-4-7", "verdict_change": "PASS->PASS",
        "duration_delta_seconds": -160, "input_tokens_delta": -4000,
        "category": "soft_improvement_by_cost" }
    ]
  }
}
```

## Categorization rules

| verdict_change | duration / token Δ | category |
|----------------|---------------------|----------|
| FAIL → PASS    | any                  | improvement |
| PASS → FAIL    | any                  | regression |
| PASS → PASS    | > 15% drop in cost   | soft_improvement_by_cost |
| PASS → PASS    | > 15% rise in cost   | soft_regression_by_cost |
| PASS → PASS    | within 15%           | neutral |
| FAIL → FAIL    | drop                 | partial_improvement_inconclusive |
| FAIL → FAIL    | rise                 | inconclusive |
| any → ERROR    |                      | inconclusive (infra issue) |
| any with hygiene_violation true | | inconclusive_hygiene |

> 15% threshold rationale: smaller deltas are noise on single-run
> evaluations. For higher confidence, set `k=2` or `k=3` per cell
> in the task spec; the orchestrator will average.

## Reviewer-verifiability score

| Score | Meaning | Example |
|-------|---------|---------|
| 2 | Deterministic | `./gradlew :polaris-core:test --tests "..."` exits 0 |
| 1 | Narrow-deterministic | output contains exact string, file count matches |
| 0 | LLM-judge | a witness LLM rates the output; subjective |

Prefer score 2. Polaris being gradle + JUnit + AssertJ makes
score-2 verifiers easy.

## Cost / time guardrails

Per-cell cap (terminate worker if exceeded):
- 200,000 total tokens (input + output)
- 20 minutes wall time
- $2.00 cost (claude-opus-4-7 reference)

Per-run total cap (refuse to start without confirmation):
- ~$5.00 expected total. Compute: `n_tasks × n_arms × n_models × n_cli × ~$0.50`.

## What we deliberately do NOT measure

- Subjective code quality — delegate to `review` / `simplify` /
  `review-tech-doc` if available.
- Long-range effects ("did this change make the codebase easier
  to navigate in 6 months?") — unmeasurable from a single run.
- Recursive maintainability of worker outputs — track size and
  test-coverage delta as proxies; deeper analysis is OOS.
