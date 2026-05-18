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

# Apache Polaris — Coding Agent Guidelines

Rules for AI coding agents contributing to Apache Polaris. Human contributors may
also find these useful, but the primary audience is automated agents (Claude Code,
Codex, Cursor, Copilot, and others).

A human author is responsible for every change an agent produces. See
[CONTRIBUTING.md](CONTRIBUTING.md#guidelines-for-ai-assisted-contributions) for the
full AI-assisted contribution policy.

This document has two types of rules:
- **Hard gate** — mechanically verifiable, blocking. If it fails, do not submit.
- **Discipline** — behavioral rule for how you work. Not mechanically verifiable,
  but violations produce the kind of PRs that reviewers reject on sight.

---

## Understand Before Coding [DISCIPLINE]

Read before you write. The most common agent failure mode is making wrong
assumptions and running with them without checking.

1. **Read the relevant code first.** Before changing a file, read it. Before
   implementing an interface, find existing implementations and study them. Before
   adding a utility, search the codebase for one that already exists.
2. **Identify existing patterns.** Polaris has established patterns for CDI wiring,
   request-scoped holders, persistence SPIs, and extension modules. Match them. If
   you are unsure what pattern applies, look at two or three similar files and follow
   what they do. If in doubt, ask the user.
3. **Plan multi-step tasks.** For changes that span more than one file, write a brief
   plan before coding:
   ```
   1. [Step] — verify: [how you'll check it worked]
   2. [Step] — verify: [how you'll check it worked]
   ```
4. **Flag contradictions.** If the task requirements conflict with existing code
   patterns or design, say so in the PR description. Do not silently comply with
   something that looks wrong.
5. **State assumptions.** If you cannot verify an assumption (expected behavior,
   intended scope, ambiguous requirements), state it explicitly in the PR description.
   This is especially important for agents running in batch mode without interactive
   feedback.
6. **Investigate before editing.** When debugging, read, search, and trace the code
   to build a diagnosis first. Speculative source edits obscure the original problem
   and risk regressions. Temporary debug output (e.g., `System.out.println`) is fine
   as long as it is removed before committing.

---

## Hard Gates

These MUST pass before you open a pull request or declare a task complete. Treat any
failure as a blocking error.

1. **Format and compile.** [HARD GATE]
   `./gradlew format compileAll` from the repo root. Fix every compilation error.
   Every new source file MUST include the ASF license header — copy from
   `codestyle/copyright-header-java.txt` for `.java` and `.kts` files,
   `codestyle/copyright-header-hash.txt` for `.properties` and `.yml`. For other
   types, check `codestyle/` for the matching template. Add headers manually;
   `./gradlew check` includes the `rat` audit as a safety net.
2. **Run module checks and tests.** [HARD GATE]
   `./gradlew :<module>:check` for each module you touched. This runs tests,
   Checkstyle (banned imports), and Spotless verification in one step. If you
   changed a module that other modules depend on (e.g., `polaris-core`), run
   `./gradlew check` (without module prefix) to catch downstream breakage.

---

## Build and Test

**Language:** Java 21 (server modules), Java 17 (client modules).
**Build system:** Gradle with Kotlin DSL. **Framework:** Quarkus with CDI.

```bash
# Full build with all checks (formatting, checkstyle, errorprone, tests)
./gradlew check

# Compile only (no tests)
./gradlew compileAll

# Format code (Google Java Format via Spotless)
./gradlew format

# Format + compile (recommended before pushing)
./gradlew format compileAll

# Run a specific module's checks and tests
./gradlew :polaris-runtime-service:check

# Run a specific test class
./gradlew :polaris-core:test \
  --tests "org.apache.polaris.core.SomeTest"

# Run a specific test method
./gradlew :polaris-core:test \
  --tests "org.apache.polaris.core.SomeTest.testMethod"
```

Error Prone runs during compilation. Fix every Error Prone error rather than
suppressing checks with `@SuppressWarnings`. Do not introduce new `-Xlint` compiler
warnings (unchecked casts, deprecation usage). [DISCIPLINE]

Module names map to directory paths via `gradle/projects.main.properties`. See
[README.md](README.md) for a breakdown of all modules.

---

## Code Style

**Formatter:** Google Java Format via Spotless. `./gradlew format` applies it.
Do not manually adjust the formatter's output.

**Imports:** Use import statements. Never inline fully-qualified class names.
[DISCIPLINE]

```java
import jakarta.enterprise.context.ContextNotActiveException;

// correct
} catch (ContextNotActiveException e) {

// wrong
} catch (jakarta.enterprise.context.ContextNotActiveException e) {
```

**Assertions:** Use AssertJ (`assertThat`) for test assertions. [DISCIPLINE]

```java
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
```

**Tests:** Favor parameterized tests when the same behavior is tested across
different inputs. [DISCIPLINE]

**Banned imports:** Checkstyle blocks all `*.shaded.*` imports and `*.relocated.*`
imports (with a narrow exception) — see `codestyle/checkstyle.xml` for details.
[HARD GATE — `./gradlew check` catches this]

---

## Naming

### Test methods [DISCIPLINE]

Match the naming style of the file you are editing. The project uses both `testXxx`
camelCase and BDD-style `verbNounExpectation`:

```java
void testCaptureWhenScopeNotActiveReturnsNull()   // testXxx style
void pathSegmentRejectsNullEntityType()            // BDD style
void capture_whenScopeNotActive_returnsNull()      // wrong — no underscores
```

### Commit messages and PR titles [DISCIPLINE]

Follow the conventions in
[CONTRIBUTING.md](CONTRIBUTING.md#code-contribution-guidelines). The PR title
becomes the squash-merge commit subject. Write it as if it will appear in release
notes: describe the feature or bug for users, not the action.

---

## PR Description [DISCIPLINE]

Use the PR template (`.github/pull_request_template.md`). At minimum:

1. Describe **why** the change is needed, not just what changed.
2. Link the issue: `Fixes #NNN` or `Related to #NNN`.
3. State your understanding of the current behavior and what you expect to change.
4. Complete every checklist item.
5. If the change affects user-facing behavior, update `CHANGELOG.md` under
   `## [Unreleased]` in the appropriate subsection — consult existing entries
   for the right category.
6. If your change affects user-facing behavior or configuration, check whether
   `site/content/in-dev/unreleased/` needs updates.

---

## Scope Discipline [DISCIPLINE]

Change only what the task requires.

- Do not reformat files you did not otherwise modify.
- Do not refactor adjacent code that "could be better."
- Do not add features, abstractions, or error handling beyond the task.
- Do not add or update dependencies unless the task explicitly requires it.
- **Clean up your own orphans.** Remove imports, variables, or methods that YOUR
  changes made unused. Do not remove pre-existing dead code unless the task asks
  for it.
- **Verify scope.** Run `git diff --stat` and check that only expected files
  appear. This requires judgment — if the task is "fix bug in RequestIdFilter" and
  the diff shows 10 unrelated files, something went wrong.

If you notice something worth improving outside the current scope, mention it in
the PR description or file a separate issue. Do not bundle it.

---

## Simplicity [DISCIPLINE]

Write the minimum code that solves the problem.

- No abstractions for single-use code.
- No "flexibility" or "configurability" that was not requested.
- No error handling for scenarios that cannot happen.
- If you wrote 200 lines and it could be 50, rewrite it.

Start with the simplest correct implementation. Optimize only when the task
requires it, and only after the simple version works and has tests.

---

## Verification Workflow

Run this sequence before declaring work complete. Steps marked HARD GATE are
blocking; steps marked DISCIPLINE are behavioral checks that prevent common
rejections.

```
1. Run the Hard Gates above             — format, compile, add headers, check
2. Re-read every changed file           — [DISCIPLINE] self-review:
                                            - Any conceptual errors?
                                            - Could this be simpler?
                                            - Does it match existing patterns?
3. git diff --stat                      — [DISCIPLINE] only expected files changed?
4. Check orphaned code                  — [DISCIPLINE] did your changes create
                                            unused imports/variables? Remove them.
```

If any HARD GATE step fails, fix the issue and restart from step 1.
If a DISCIPLINE step reveals a problem, fix it and re-run the HARD GATE steps.

---

## Common Mistakes

**Changing interface method signatures.** [DISCIPLINE]
Changes to public interfaces or extension points are significant design decisions
that require prior discussion on the dev mailing list, see
[CONTRIBUTING.md](CONTRIBUTING.md#code-contribution-guidelines). See also the
[evolution guidelines](site/content/in-dev/unreleased/evolution.md) for the
project's approach to API and code changes.

**Inventing new patterns.** [DISCIPLINE]
Check the codebase for existing patterns before introducing new utility classes or
abstractions. Follow what is already there.

**Silent compliance with bad requirements.** [DISCIPLINE]
If a task asks you to do something that contradicts existing code patterns, do not
silently comply. Explain the contradiction in the PR description and propose an
alternative. Reviewers would rather see a thoughtful objection than a PR that
introduces inconsistency.

**AI self-references in output.** [DISCIPLINE]
Do not include AI tool names or model identifiers in code, comments, or commit
messages. For PR descriptions, follow the disclosure guidance in
[CONTRIBUTING.md](CONTRIBUTING.md#guidelines-for-ai-assisted-contributions).
The human author owns the contribution.
