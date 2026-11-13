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
in this directory. Separate fragment files avoid conflicts caused by unrelated pull requests
inserting entries at the same position in `CHANGELOG.md`.

Add an entry only for changes worth calling out to users in the release notes, such as breaking
changes, new features, upgrade caveats or notable fixes.

## Adding an entry

Create a file named `changelog.d/<short-title>.md`, where `<short-title>` is a short, unique,
lowercase title with words separated by dashes, for example `namespace-location-error.md`.

The file looks like a miniature changelog. It has one section for each part of the changelog that
the change needs an entry in. A section starts with a heading naming it, followed by one or more
list items, written like in `CHANGELOG.md`:

* Each item starts with `- `.
* The other lines of an item, such as wrapped text, paragraphs, nested lists or code blocks, are
  indented by two spaces.

The sections are the ones of the changelog: `Highlights`, `Upgrade notes`, `Breaking changes`,
`New Features`, `Changes`, `Deprecations` and `Fixes`. Fragments have no license header.

A fix needs one section:

```markdown
### Fixes

- Setting a namespace's `location` property no longer fails with an internal error when its parent
  is itself a nested namespace that has no stored location.
```

A breaking change typically needs two:

```markdown
### Breaking changes

- `LIST_PAGINATION_ENABLED` now defaults to true. List APIs honor pagination parameters.

### Upgrade notes

- Set `LIST_PAGINATION_ENABLED` to false to keep the previous behavior.
```

The items are added to the sections of `CHANGELOG.md` as written. Every line that does not start
with a space is therefore either blank, a `- ` item, or a section heading, which is what the
validation checks.

Within a section, entries are listed in the order of the file names of their fragments. Follow-up
pull requests can edit or delete fragments added by earlier ones.

`./gradlew checkChangelogFragments` validates the fragments; it also runs as part of
`./gradlew check`, and in CI for every pull request that changes this directory.

## Releasing

When a release is cut, `./gradlew releaseChangelog --release-version=x.y.z` appends the fragments
to the matching sections of `## [Unreleased]` in `CHANGELOG.md` and deletes them. It then turns that
section into the one of the release, adds a new empty `## [Unreleased]` section, drops the
subsections without entries from the release, and updates the links at the end of the file. Once the
release vote has passed, the release manager propagates these changes of `CHANGELOG.md`, including
the links at the end of the file, and the deletion of the consumed fragments to `main`. The release
guides describe how, and what changes for a release that is not cut from `main`.

For later release candidates, fragments added by cherry-picked changes are merged into the existing
section of the release with `./gradlew mergeChangelogFragments --release-version=x.y.z`. The
release workflow does this automatically and fails if any fragment remains.

Both tasks validate everything before they write anything, then replace `CHANGELOG.md` atomically
and delete the fragments. If a fragment cannot be deleted, the task fails and lists the remaining
fragments: `CHANGELOG.md` already contains their entries, so delete those files manually and do not
run the task again, which would add the entries twice.
