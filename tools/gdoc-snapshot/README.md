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

# Google Docs proposal snapshots (PoC)

Keep drafting and reviewing in Google Docs, then archive a selected version in
Git as Markdown with local images. Each proposal has a manifest connecting its
version label, ordinary Google Doc link, Google revision, and snapshot paths.
Readers can open the committed Markdown without a Google account.

This is a PoC for the [proposal-docs discussion](https://lists.apache.org/thread/yto2wp982t43h1mqjwnslswhws5z47cy).
The [Tag proposal example](../../proposals/tag-management/README.md) contains a
real captured export. Archiving a document does not mark the proposal accepted.
Community decisions and discussion summaries still belong on the dev list.

## Try the included example offline

From the repository root, on Linux or macOS with Python 3.10 or newer:

```sh
python3 tools/gdoc-snapshot/gdoc_snapshot.py check proposals/tag-management/manifest.json
python3 -m unittest discover -s tools/gdoc-snapshot -p 'test_*.py' -v
```

No third-party Python packages, Google login, or network access are needed for
these checks. `check` verifies all recorded revisions, source metadata, relative
paths, bundle hashes, and every file hash, including images.

## Capture your own proposal

1. In Google Docs, set **Share → General access → Anyone with the link → Editor**.
   This PoC requires that setting for anonymous revision discovery and export.
   Anyone holding the link can modify the live document. Organization sharing
   restrictions may prevent this setup. The tool never changes permissions.
2. Create a proposal manifest and import the ordinary Google Doc URL:

   ```sh
   mkdir -p proposals/my-proposal
   cat > proposals/my-proposal/manifest.json <<'JSON'
   {"format": 1, "proposal": "my-proposal", "revisions": {}}
   JSON
   python3 tools/gdoc-snapshot/gdoc_snapshot.py import \
     proposals/my-proposal/manifest.json --version rev1 \
     --url 'https://docs.google.com/document/d/YOUR_DOCUMENT_ID/edit'
   ```

3. Review the generated `document.md`, its images, and the reported coverage
   warnings. Commit the manifest and the complete `snapshots/` directory together.
   Share the committed snapshot link alongside the live Doc in the proposal issue
   or dev-list thread.

The importer reads the anonymous page, discovers one consistent numeric revision,
and requests both Markdown and HTML exports at that revision. It uses Google's
web exporter, not an authenticated Drive API. No OAuth setup, browser cookies,
or credentials are used. These observed web endpoints are not a stable API.
Discovery or fixed-revision export failure stops the import without falling back
to an unversioned download. The import probes access by fetching content, rather
than auditing the document's sharing settings.

After editing the live Doc, capture another proposal version:

```sh
python3 tools/gdoc-snapshot/gdoc_snapshot.py import \
  proposals/my-proposal/manifest.json --version rev2
```

This discovers the current Google revision. To select an available revision
explicitly, add `--google-revision 123`. An existing label is immutable: importing
`rev1` again checks and reuses its committed bytes offline. Asking that label to
refer to another document or revision fails. Two labels selecting an already
captured Google revision reuse the same bundle.

## What is recorded

| Field | Meaning |
| --- | --- |
| `rev1` | Author-chosen proposal version, independent of Google's numbering |
| `google_doc_url` | Ordinary live Google Doc link for continued collaboration |
| `google_revision` | Numeric Google revision selected for both exports |
| `snapshot_md` | Markdown paths relative to this proposal's directory |
| `snapshot_manifest` | Relative path to source metadata, coverage, and file hashes |

Each bundle contains `document.md` for reading, `source.md` with the original
Markdown export bytes, local image assets, and its own `manifest.json`. The bundle
directory is the SHA-256 of that manifest. A single `document.md` currently contains
the whole exported document, including tabs; tabs are not split into files.

The Git snapshot preserves the imported bytes. Google may change its exporter or
stop serving an old revision. The ordinary Doc link opens the live document and
is not a historical-view link. Neither a published revision link nor a Google
history viewer is required to read the fixed snapshot.

## Fidelity and failure behavior

Embedded PNG, JPEG, GIF, and WebP images become local files. The importer compares
image and table counts with the HTML export from the same revision. Missing image
occurrences and Google Drawings produce visible warnings in Markdown, bundle
metadata, and CLI output. They do not cause the importer to invent replacement
text. Unfreezable external images, mismatched table counts, and recognized
unsupported embeds stop the import.

These checks do not prove visual or semantic equivalence. Google's Markdown
export may alter formatting, and comments and discussions are not archived.
Review the snapshot before presenting it as a design reference. The Tag example
has one local image and 65 tables, with no detected image gaps.

Before adding a version, the tool verifies every existing entry. It writes a
complete bundle before atomically replacing the proposal manifest. A failed
publication leaves existing entries intact and may leave a complete, unreferenced
bundle. Do not manually edit a committed snapshot to fix export fidelity; capture
an explicitly named new version instead. Hash verification detects accidental
changes, but does not authenticate a maliciously rewritten manifest and bundle.

## Older prototype snapshots

`migrate` can register a pinned legacy `source.json` snapshot without downloading
or changing its bytes. Create an empty proposal manifest beside `source.json`,
then run:

```sh
python3 tools/gdoc-snapshot/gdoc_snapshot.py migrate \
  proposals/my-proposal/source.json proposals/my-proposal/manifest.json --version rev1
```

Snapshots captured without a Google revision cannot be relabelled as pinned.

## PoC scope

The tool and example live in Polaris for this experiment. The workflow verifies
committed snapshots offline; it never imports from Google or automatically updates
proposals. Website publication, proposal acceptance, and comment archival are
outside this PoC. Source exports and JSON manifests are excluded from the header
audit so their recorded bytes remain intact; provenance is recorded beside the
Tag example.
