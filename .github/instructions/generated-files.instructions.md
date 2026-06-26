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

Avoid detailed review comments on generated files. If a generated file appears
stale or inconsistent, point reviewers to the likely source input or generator
instead of proposing manual edits to generated output.

For generated documentation, OpenAPI output, configuration references, Helm
schema/docs, or generated clients, prefer comments such as "Please check whether
the generator or source input needs to be updated and regenerated."

Do not ask reviewers to update generated, checked-out, or build-time-only release
documentation unless those files are present in the PR diff.
