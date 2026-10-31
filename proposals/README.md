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

# Proposal snapshot experiment

Maintain one `manifest.yaml` per proposal. Add a version and its Google Doc link.
The proposal build fills in its Google revision and relative snapshot paths.
Later builds verify frozen versions offline and only capture new entries.

| Example | Owner-maintained manifest | Read an archived version |
| --- | --- | --- |
| [Tag management](tag-management/README.md) | [manifest.yaml](tag-management/manifest.yaml) | [rev1 Markdown](tag-management/snapshots/6a14366e76ccb3b8614d25bdc22e350abd90233ef6c1147989b5485c7bc859dd/document.md) |

See [setup and commands](../tools/gdoc-snapshot/README.md). Run
`make proposal-snapshots` to preview locally, or push pending entries to a topic
branch with Actions enabled and let CI commit the generated output. README files
are documentation and are not parsed by the tool.

This is a PoC, not an adopted proposal process. Capturing a document does not imply
community acceptance or describe the current implementation. Keep decisions and
discussion summaries on the dev list.
