---
applyTo: "**"
---

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

Look for changes that may need human review for missing documentation, tests, or
security notes.

Comment only when the diff changes one of these areas and no matching update is
visible:

- security boundaries, authentication, authorization, secrets, credentials,
  trust assumptions, external integrations, storage access, network exposure, or
  privilege model
- coding-agent expectations, contribution workflow, build/test commands, review
  policy, or repository conventions
- user-visible behavior, configuration, CLI behavior, Helm behavior, APIs,
  deployment behavior, or operational guidance

Likely documentation targets include `SECURITY-THREAT-MODEL.md`, `SECURITY.md`,
`AGENTS.md`, `README.md`, `CONTRIBUTING.md`, Helm README files,
`site/content/in-dev/**`, and user-facing site docs tracked in this repository
under `site/content/**`. Do not ask reviewers to update generated or
build-time-only release docs unless those files are present in the PR diff.

When reviewing updates to an existing PR, do not repeat prior comments. Only
comment on newly introduced or newly changed risks visible in the latest diff.
