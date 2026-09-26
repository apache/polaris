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

This PoC uses EJ Wang's
[Polaris Tag proposal](https://docs.google.com/document/d/1rIJGzcsmGhfrBiRXPac51hr-jeJuuKQQBYjgBdOb9-k/edit).
Its single proposal definition is [manifest.yaml](manifest.yaml).

`rev1` preserves the previously captured Google revision `3625`. Its
[readable Markdown](snapshots/6a14366e76ccb3b8614d25bdc22e350abd90233ef6c1147989b5485c7bc859dd/document.md)
and all original capture bytes remain unchanged. The capture has one local PNG
and 65 tables, with no detected image gaps. It was made with prototype `0.4.0`.
The original tool version, hash, and source metadata remain in the
[bundle manifest](snapshots/6a14366e76ccb3b8614d25bdc22e350abd90233ef6c1147989b5485c7bc859dd/manifest.json).

`rev2` was submitted as a link-only entry to exercise the revised branch CI.
The [successful run](https://github.com/flyingImer/polaris/actions/runs/35818121693)
selected Google revision `3625`, reused the existing bundle, and
[committed the generated fields](https://github.com/flyingImer/polaris/commit/d1a25e3545ed79a92effdcdceadc53b9e3963aa6)
back to the branch. These labels demonstrate capture behavior and do not mark
an accepted Tag design.

From the repository root, after installing the tool's Python dependency:

```sh
make proposal-snapshots
make proposal-snapshots-check
```

To experiment with another capture, add a new link-only version or create a new
proposal directory using the [setup instructions](../../tools/gdoc-snapshot/README.md).

Raw capture bytes are excluded from the source-header audit because inserting a
header would alter their recorded hashes. Provenance is recorded here and in
each bundle. Count checks do not establish full fidelity, and comments and
Google Docs discussions are not archived.
