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

# Tag management: real capture example

This is an archival example from EJ Wang's
[Polaris Tag proposal](https://docs.google.com/document/d/1rIJGzcsmGhfrBiRXPac51hr-jeJuuKQQBYjgBdOb9-k/edit).
It demonstrates the snapshot workflow. It does not designate a final or accepted
Tag design, and it is not a fresh capture of the current live Doc.

| Reference | Value |
| --- | --- |
| Proposal version in this PoC | `rev1` |
| Google revision | `3625` |
| Proposal manifest | [manifest.json](manifest.json) |
| Read the snapshot | [document.md](snapshots/6a14366e76ccb3b8614d25bdc22e350abd90233ef6c1147989b5485c7bc859dd/document.md) |
| Capture metadata and hashes | [bundle manifest](snapshots/6a14366e76ccb3b8614d25bdc22e350abd90233ef6c1147989b5485c7bc859dd/manifest.json) |

The capture contains one local PNG and 65 Markdown tables, matching the image and
table counts in the HTML export. No image gaps were detected. Count checks do not
establish full fidelity; comments and discussions are not included.

The bundle was captured by prototype `0.4.0` and registered in the proposal manifest
by `0.5.0`. Its original tool version, tool hash, raw export, readable Markdown,
image bytes, and bundle hash are retained. Adding ASF headers inside these
archived files would change the evidence, so their provenance is recorded here
and the generated capture files are excluded from the header audit.

To verify every archived byte from the repository root:

```sh
python3 tools/gdoc-snapshot/gdoc_snapshot.py check proposals/tag-management/manifest.json
```

To exercise a new live import without altering this example, follow the
[tool instructions](../../tools/gdoc-snapshot/README.md) with a new proposal
directory and the live Tag Doc URL above.
