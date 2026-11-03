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

# Changelog fragments

Pull requests do not edit `CHANGELOG.md` directly. Instead, each changelog entry is a small file
in this directory. Since two pull requests never touch the same file, changelog entries never
cause merge conflicts.

Add an entry only for changes worth calling out to users in the release notes, such as breaking
changes, new features, upgrade caveats or notable fixes.

## Adding an entry

Create a file named `changelog.d/<section>/NN-<short-title>.md`, where:

* `<section>` is one of the sections of the changelog:

  | Directory          | Section in `CHANGELOG.md` |
  |--------------------|---------------------------|
  | `highlights`       | Highlights                |
  | `upgrade-notes`    | Upgrade notes             |
  | `breaking-changes` | Breaking changes          |
  | `new-features`     | New Features              |
  | `changes`          | Changes                   |
  | `deprecations`     | Deprecations              |
  | `fixes`            | Fixes                     |

* `NN` is a two-digit priority: entries with smaller numbers are listed first in their section.
  Use `99` unless the position of the entry matters.
* `<short-title>` is a short, unique, lowercase title with words separated by dashes. It also
  orders entries that have the same priority.

The file contains the text of a single list item, in plain Markdown, without the leading `- `
list marker and without a license header. Long lines can be wrapped. For example,
`changelog.d/fixes/99-namespace-location-internal-error.md`:

```markdown
Setting a namespace's `location` property no longer fails with an internal error when its parent
is itself a nested namespace that has no stored location.
```

A change that needs entries in several sections uses one file per section. Follow-up pull requests
can edit or delete fragments added by earlier ones.

`./gradlew checkChangelogFragments` validates the fragments; it also runs as part of
`./gradlew check`, and in CI for every pull request that changes this directory.

## Releasing

When a release is cut, `./gradlew mergeChangelogFragments` appends the fragments to the matching
sections of `## [Unreleased]` in `CHANGELOG.md` and deletes them, before `./gradlew patchChangelog`
turns that section into the release's section. The deletion of the fragments is then propagated
to `main` together with the `CHANGELOG.md` update.
