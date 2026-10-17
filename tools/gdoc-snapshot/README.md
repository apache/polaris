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

A proposal owner maintains one `manifest.yaml`. Add a version label and an ordinary
Google Doc link. The build fills in the Google revision and local snapshot paths
in that same entry. Existing versions stay frozen.

This PoC follows the [proposal-docs discussion](https://lists.apache.org/thread/yto2wp982t43h1mqjwnslswhws5z47cy).
The [Tag example](../../proposals/tag-management/README.md) uses a real proposal.
Capturing content does not mark a proposal accepted. Community decisions and
discussion summaries still belong on the dev list.

## Owner workflow

In Google Docs, set **Share / General access / Anyone with the link / Editor**.
The anonymous importer requires this setting. Anyone holding the link can edit
the live Doc. The tool never changes sharing permissions or requires Google OAuth.
Organization restrictions may prevent this setup.

Create `proposals/my-proposal/manifest.yaml` with the repository's ASF comment
header and this content:

```yaml
proposal: my-proposal
revisions:
  rev1:
    google_doc_url: https://docs.google.com/document/d/YOUR_DOCUMENT_ID/edit
```

Commit and push it to a topic branch in your fork. With GitHub Actions enabled,
branch CI captures pending entries and pushes a generated commit to that same
branch. Pull that commit before continuing locally. No separate import command,
input file, or manually created snapshot directory is needed.

For a local preview, use Python 3.10+ on Linux or macOS:

```sh
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r tools/gdoc-snapshot/requirements.txt
make proposal-snapshots
make proposal-snapshots-check
```

The local build writes files for review and does not commit automatically. Commit
the filled YAML and snapshots with your change if you want those exact preview
bytes preserved. If you push only the pending YAML, CI captures what the Doc
contains when CI runs. PyYAML is the only Python dependency.

After the live Doc changes, add another label with only `google_doc_url`:

```yaml
  rev2:
    google_doc_url: https://docs.google.com/document/d/YOUR_DOCUMENT_ID/edit
```

Do not copy the generated fields from the previous entry. Multiple pending labels
for the same Doc in one build select the same revision. To archive different
moments, complete one capture before adding the next version.

## One manifest, two entry states

| Entry | Build behavior |
| --- | --- |
| Only `google_doc_url` | Capture once and add all generated fields |
| All generated fields present | Verify the archived bytes offline |
| Partial generated fields or unknown fields | Fail with an explicit error |

The generated fields are `google_revision`, `snapshot_md`, and
`snapshot_manifest`. Both paths are relative to the directory containing the YAML.
A completed entry looks like this (paths abbreviated):

```yaml
  rev1:
    google_doc_url: https://docs.google.com/document/d/YOUR_DOCUMENT_ID/edit
    google_revision: '3625'
    snapshot_md:
      - snapshots/<hash>/document.md
    snapshot_manifest: snapshots/<hash>/manifest.json
```

The YAML is reformatted when entries are filled, so free-form YAML comments are
not retained. Put narrative notes in README.md. A build with no pending entries
does not rewrite the YAML or fetch Google. Duplicate YAML keys are rejected.

Builds compare frozen entries with Git `HEAD` by default. CI supplies the previous
push commit or PR base commit. Clearing generated fields, changing a frozen entry,
or removing an archived proposal fails rather than recapturing it. For an explicit
comparison, use `make proposal-snapshots PROPOSAL_BASE_REF=<commit>`.

## Capture and validation

The importer discovers one numeric Google revision from the anonymous document
page, then requests Google's Markdown and HTML exports at that revision. These
are observed web endpoints, not a stable public API. Discovery or fixed-revision
export failure stops the build without an unversioned fallback.

Each bundle contains readable `document.md`, the original Markdown export as
`source.md`, local images, and a JSON manifest with provenance, coverage, and file
hashes. That JSON is generated bundle metadata, not a second owner-maintained
proposal definition. The bundle directory is the SHA-256 of its manifest bytes.
The whole exported document currently becomes one Markdown file, including tabs.

The ordinary Doc link opens the live document. The committed snapshot is the
fixed reference. No Google historical-view or published-revision link is needed.
Google may later change its exporter or stop serving an old revision, so builds
reuse existing captured bytes.

Embedded PNG, JPEG, GIF, and WebP images become local assets. The importer compares
image and table counts with the HTML export from the same revision. Missing image
occurrences and Google Drawings produce visible warnings in Markdown, bundle
metadata, and CLI output. Unfreezable external images, mismatched table counts,
and recognized unsupported embeds stop capture. Comments are not archived.
Count checks do not prove semantic or visual fidelity. Review generated content
before using it as a design reference.

All input manifests and existing bundles are checked before network access. Within
each proposal, all pending entries must succeed before its YAML is atomically
replaced. A failure can leave an unreferenced bundle, or completed output for an
earlier proposal. CI commits nothing unless the entire build and check succeed.

## CI setup and scope

Enable GitHub Actions in your fork. The workflow requests `contents: write` only
for its capture job, which runs on pushes to non-default branches in that same
repository. Repository or organization policy can still prohibit writes. The
capture job uses the repository's `GITHUB_TOKEN`, not a personal access token.

PR jobs only validate and have read permissions. An upstream PR workflow cannot
write back to a contributor's fork. This is why automatic capture runs in the
owner's fork before or alongside opening the PR. A fork with Actions disabled must
use the local build instead. The workflow never runs PR code with upstream write
credentials, and never force-pushes over a branch that advanced during capture.

Bot pushes do not recursively start push workflows. GitHub may require a maintainer
to approve PR workflows after a bot update. See
[GitHub's token behavior](https://docs.github.com/en/actions/concepts/security/github_token)
and [fork PR permissions](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request).

The dedicated workflow runs only proposal tests, capture, and offline validation.
It does not compile Polaris. Website publication, proposal acceptance, and comment
archival are outside this PoC.

To run the controlled behavior tests:

```sh
python3 -m unittest discover -s tools/gdoc-snapshot -p 'test_*.py' -v
```

The raw capture files have narrow RAT exclusions to preserve their recorded bytes.
The YAML and all new code and documentation carry ASF headers. There is no legacy
`source.json` migration interface or fixture set.
