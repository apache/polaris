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

This directory demonstrates storing Google Docs proposal snapshots in the Polaris
repository. It is a PoC for community feedback, not an adopted proposal process.

| Example | Live source | Captured version | Read in Git |
| --- | --- | --- | --- |
| [Tag management](tag-management/README.md) | [Google Doc](https://docs.google.com/document/d/1rIJGzcsmGhfrBiRXPac51hr-jeJuuKQQBYjgBdOb9-k/edit) | `rev1` / Google revision `3625` | [Markdown snapshot](tag-management/snapshots/6a14366e76ccb3b8614d25bdc22e350abd90233ef6c1147989b5485c7bc859dd/document.md) |

See the [tool's setup and commands](../tools/gdoc-snapshot/README.md) to try it.
The [Tag manifest](tag-management/manifest.json) maps the live document and Google
revision to files relative to the proposal directory. Existing version labels and
snapshot files stay unchanged when a new version is added.

Keep community decisions and discussion summaries on the dev list. A snapshot
records content at capture time and does not imply proposal acceptance or describe
the current implementation.
