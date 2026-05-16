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
# Context hygiene for Polaris eval workers

The core invariant: a worker subagent running a task arm must have
**only** what a real fresh agent in the field would have — the
worktree contents at the assigned ref, plus optionally
`references/polaris-context.md` if the task opted in. Everything
else is contamination.

## The hygiene preamble

Every worker prompt must begin with this block, verbatim:

```
You are a fresh agent evaluating a Polaris coding task. Follow these
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
   - Do not run `env`, `printenv`, `set` (without args), `export -p`,
     `declare -p`, `compgen -e`, or shell-builtins that dump env.
   - Do not read `/proc/self/environ`, `/proc/$$/environ`,
     `/proc/*/environ`, or any other path that exposes process env.
   - Do not include the value of any environment variable in your
     output, self-report, comments, file contents, or commit
     messages — not even ones that look benign (PATH, HOME, etc.).
   The harness passes auth credentials (ANTHROPIC_API_KEY,
   AWS_*, OPENAI_API_KEY, GOOGLE_APPLICATION_CREDENTIALS, …) to
   your CLI via process env so it can reach its inference endpoint.
   Reading those values back into your transcript would exfiltrate
   them. If a task seems to require an env-var value, treat that as
   an unresolved gap (rule 2) and stop.

5. You may and should: read AGENTS.md, .agents/skills/<any>/SKILL.md,
   CLAUDE.md, .codex/instructions.md, and any source/test files
   inside $WORKTREE. Run `./gradlew` and `git` commands. Execute
   the task's verifier.

6. When finished, print:
     ## Outcome
     <one-paragraph summary>
     ## Files changed
     <list>
     ## Verifier
     <command you ran and its exit code>
```

## Contamination sources (ranked)

### 1. Skill-registry / rule-registry leak (CONFIRMED)

Claude Code (and similar CLIs) inject an "available skills" or
"loaded rules" system-reminder into every subagent, built from the
parent session's installed skill list — NOT from the subagent's
worktree. If the change under eval adds or modifies a skill (or
AGENTS.md, or `.codex/`, or `.cursor/`), the BEFORE worker will see
the new content's description in its system-reminder even though
the file is absent from its worktree.

This was observed empirically in an earlier iteration of this
harness: a BEFORE worker mid-task noticed the reminder and rewrote
its answer to reference the "new" skill that its worktree did not
contain.

**Mitigations (strongest first):**

a. **Fresh-process CLI dispatch.** Spawn a brand-new agent CLI
   subprocess whose registry rebuilds from
   `$WORKTREE/.agents/` and `$WORKTREE/.claude/` only. Run with
   `env -i HOME=/tmp/sb_<runid> PATH=/usr/bin:/bin` and pass
   through only the credentials the CLI needs. `scripts/run_arm.sh`
   does this.

b. **Defer-install pattern.** Persist the new skill files only
   into the AFTER worktree (apply as a patch). BEFORE never sees
   them on disk OR in registry. More finicky but works without an
   external CLI.

c. **Post-hoc subtraction.** Accept the leak; only score behaviors
   the BEFORE worker could not have learned from the registry
   blurb. Use content-depth verifiers: ones that require the
   worker to have actually read the SKILL.md body or AGENTS.md
   section, not just notice that one exists.

### 2. User-global agent config files

Known paths to block in the preamble:
- `$HOME/.claude/CLAUDE.md`, `$HOME/.claude/settings.json`,
  `$HOME/.claude/skills/**`, `$HOME/.claude/projects/*/memory/`
- `$HOME/.codex/instructions.md`, `$HOME/.codex/config.toml`,
  `$HOME/.codex/prompts/`
- `$HOME/.cursor/rules/`, `$HOME/.cursor/mcp.json`
- `$HOME/.copilot/`, `$HOME/.config/copilot/`

In-prompt wording stops explicit reads. Auto-loaded user CLAUDE.md
is not stoppable from inside the worker — only fresh-process
dispatch with `env -i HOME=...` blocks it.

### 3. Parent conversation spillover

The Agent tool spawns with a fresh context window in-process, but
skill descriptions and other system-reminder content visible to
the parent are re-emitted to the subagent. Same mitigation (a).

## Post-run checks

After both arms finish, before scoring:

- Verify each arm's `files changed` list contains no path outside
  the assigned worktree.
- Grep each arm's self-reported reasoning + any agent transcript
  for tokens that appear in the change diff (between BEFORE and
  AFTER) but not in BEFORE's worktree tree. Any match → flag the
  run with `hygiene_violation` and treat the affected metric as
  inconclusive.
- Specifically: if AGENTS.md changed, grep for the new section's
  heading or distinctive phrases.
