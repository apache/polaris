#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Exercise the same YAML build/check interface used locally and in CI."""
import copy
import json
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import gdoc_snapshot as g

URL = "https://docs.google.com/document/d/example_doc/edit"
PAGE = b'<script>DOCS_warmStartDocumentLoader.startLoad(12.0,"anonymous");</script><script>DOCS_modelChunk = {"revision":12};</script>'
MD = b"# Example\n\nFrozen text.\n"
HTML = b"<h1>Example</h1><p>Frozen text.</p>"


class SnapshotBuild(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        self.root = self.repo / "proposals"
        self.path = self.root / "example/manifest.yaml"
        self.path.parent.mkdir(parents=True)
        self.write({"proposal": "example", "revisions": {"rev1": {"google_doc_url": URL}}})

    def write(self, proposal):
        self.path.write_bytes(g.proposal_bytes(proposal))

    def fetched(self, url, kind):
        if kind != "page":
            self.assertIn("revision=12", url)
        return {"page": PAGE, "md": MD, "html": HTML}[kind]

    def first(self):
        with patch.object(g, "fetch", self.fetched):
            return g.build_all(self.root)

    def pending(self, version="rev2", url=URL):
        proposal = g.load_proposal(self.path)
        proposal["revisions"][version] = {"google_doc_url": url}
        self.write(proposal)

    def tree(self):
        return {str(p.relative_to(self.root)): p.read_bytes() for p in self.root.rglob("*")
                if p.is_file() and p.name != ".gdoc-import.lock"}

    def newer(self, url, kind):
        if kind != "page":
            self.assertIn("revision=13", url)
        return {"page": PAGE.replace(b"12", b"13"), "md": b"# New content\n", "html": b"<h1>New content</h1>"}[kind]

    def expect_failure(self, code, operation):
        with self.assertRaises(g.Failure) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repo), *args], stderr=subprocess.STDOUT).decode().strip()

    def commit(self):
        self.git("add", "proposals")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "Capture example")
        return self.git("rev-parse", "HEAD")

    def init_git(self):
        self.git("init", "-q")
        return self.commit()

    def test_build_enriches_one_yaml_and_frozen_build_is_byte_identical_offline(self):
        self.first()
        proposal = g.load_proposal(self.path)
        entry = proposal["revisions"]["rev1"]
        self.assertEqual(entry["google_doc_url"], URL)
        self.assertEqual(entry["google_revision"], "12")
        self.assertIn(b"Frozen text", (self.path.parent / entry["snapshot_md"][0]).read_bytes())
        before = self.tree()
        with patch.object(socket.socket, "connect", side_effect=AssertionError("NETWORK USED")):
            result = g.build_all(self.root)
            self.assertEqual(result[str(self.path)]["status"], "unchanged")
            g.build_all(self.root, check_only=True)
        self.assertEqual(before, self.tree())

    def test_new_version_preserves_old_entry_and_all_archived_bytes(self):
        self.first()
        old = copy.deepcopy(g.load_proposal(self.path)["revisions"]["rev1"])
        self.pending()
        before = self.tree()
        with patch.object(g, "fetch", self.newer):
            g.build_all(self.root)
        proposal = g.load_proposal(self.path)
        self.assertEqual(proposal["revisions"]["rev1"], old)
        self.assertEqual(proposal["revisions"]["rev2"]["google_revision"], "13")
        for name, data in before.items():
            if name != "example/manifest.yaml":
                self.assertEqual((self.root / name).read_bytes(), data)

    def test_same_google_revision_reuses_existing_bundle(self):
        self.first()
        self.pending()
        with patch.object(g, "fetch", side_effect=lambda url, kind: PAGE if kind == "page" else self.fail("Export was repeated")):
            g.build_all(self.root)
        entries = g.load_proposal(self.path)["revisions"]
        self.assertEqual(entries["rev1"]["snapshot_md"], entries["rev2"]["snapshot_md"])

    def test_multiple_pending_labels_for_one_document_share_one_capture(self):
        self.pending()
        calls = []
        def capture(url, kind):
            calls.append(kind)
            return self.fetched(url, kind)
        with patch.object(g, "fetch", capture):
            g.build_all(self.root)
        self.assertEqual(calls, ["page", "md", "html"])
        self.assertEqual(len(list((self.path.parent / "snapshots").iterdir())), 1)

    def test_check_reports_pending_without_downloading(self):
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.expect_failure("PENDING_SNAPSHOT", lambda: g.build_all(self.root, check_only=True))

    def test_partial_or_unknown_fields_fail_before_network(self):
        original = g.load_proposal(self.path)
        for fields in ({"google_revision": "12"}, {"snapshot_md": []}, {"typo": "x"}):
            proposal = copy.deepcopy(original)
            proposal["revisions"]["rev1"].update(fields)
            self.write(proposal)
            with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
                self.expect_failure("PARTIAL_ENTRY", lambda: g.build_all(self.root))

    def test_discovery_or_download_failure_does_not_publish_or_fall_back(self):
        before = self.tree()
        for page in (b"<html>new Google layout</html>", PAGE.replace(b'"revision":12', b'"revision":13')):
            calls = []
            def fetch(url, kind):
                calls.append(kind)
                return page
            with patch.object(g, "fetch", fetch):
                self.expect_failure("REVISION_DISCOVERY_FAILED", lambda: g.build_all(self.root))
            self.assertEqual(calls, ["page"])
        def failed_export(url, kind):
            if kind == "page":
                return PAGE
            self.assertIn("revision=12", url)
            raise g.Failure("ANONYMOUS_FETCH_FAILED", "HTTP 400")
        with patch.object(g, "fetch", failed_export):
            self.expect_failure("ANONYMOUS_FETCH_FAILED", lambda: g.build_all(self.root))
        self.assertEqual(self.tree(), before)

    def test_later_pending_capture_failure_keeps_whole_manifest_unpublished(self):
        self.pending(url=URL.replace("example_doc", "other_doc"))
        before = self.path.read_bytes()
        def fetch(url, kind):
            if "other_doc" in url:
                raise g.Failure("NETWORK_ERROR", "unavailable")
            return self.fetched(url, kind)
        with patch.object(g, "fetch", fetch):
            self.expect_failure("NETWORK_ERROR", lambda: g.build_all(self.root))
        self.assertEqual(self.path.read_bytes(), before)

    def test_failed_atomic_publication_preserves_old_history_and_releases_lock(self):
        self.first()
        self.pending()
        before = self.path.read_bytes()
        with patch.object(g, "fetch", self.newer), patch.object(g.os, "replace", side_effect=OSError("publication failure")):
            with self.assertRaises(OSError):
                g.build_all(self.root)
        self.assertEqual(self.path.read_bytes(), before)
        g.check_entry(self.path.parent, "rev1", g.load_proposal(self.path)["revisions"]["rev1"])
        self.assertEqual(list(self.path.parent.glob(".gdoc-manifest-*")), [])
        self.assertEqual(list((self.path.parent / "snapshots").glob(".gdoc-stage-*")), [])
        with (self.path.parent / ".gdoc-import.lock").open("a+b") as stream:
            g.fcntl.flock(stream, g.fcntl.LOCK_EX | g.fcntl.LOCK_NB)

    def test_concurrent_manifest_edit_is_not_overwritten(self):
        user_bytes = self.path.read_bytes().replace(b"proposal: example", b"proposal: edited")
        def fetch(url, kind):
            self.path.write_bytes(user_bytes)
            return self.fetched(url, kind)
        with patch.object(g, "fetch", fetch):
            self.expect_failure("SOURCE_CHANGED", lambda: g.build_all(self.root))
        self.assertEqual(self.path.read_bytes(), user_bytes)

    def test_corrupted_old_bundle_blocks_new_capture_before_network(self):
        self.first()
        entry = g.load_proposal(self.path)["revisions"]["rev1"]
        (self.path.parent / entry["snapshot_md"][0]).write_text("changed")
        self.pending()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.expect_failure("FILE_HASH_MISMATCH", lambda: g.build_all(self.root))

    def test_changed_source_and_escaping_paths_are_rejected(self):
        self.first()
        original = g.load_proposal(self.path)
        for key, value, code in [("google_revision", "13", "SOURCE_MISMATCH"),
                                 ("google_doc_url", URL.replace("example_doc", "other"), "SOURCE_MISMATCH"),
                                 ("snapshot_md", ["../../outside.md"], "SNAPSHOT_PATH_MISMATCH"),
                                 ("snapshot_manifest", "../manifest.json", "INVALID_PATH")]:
            proposal = copy.deepcopy(original)
            proposal["revisions"]["rev1"][key] = value
            self.write(proposal)
            self.expect_failure(code, lambda: g.build_all(self.root))

    def test_missing_images_warn_and_local_assets_are_hashed(self):
        md = b"![image](data:image/png;base64,iVBORw0KGgo=)\n"
        html = b'<img src="inline"><img src="https://docs.google.com/drawings/d/example/image">'
        with patch.object(g, "fetch", side_effect=lambda url, kind: {"page": PAGE, "md": md, "html": html}[kind]):
            g.build_all(self.root)
        entry = g.load_proposal(self.path)["revisions"]["rev1"]
        document = self.path.parent / entry["snapshot_md"][0]
        self.assertTrue(document.read_text().startswith("> **Image coverage warning**"))
        self.assertIn("assets/", document.read_text())
        self.assertNotIn("data:image/", document.read_text())
        next((document.parent / "assets").iterdir()).write_bytes(b"changed")
        self.expect_failure("FILE_HASH_MISMATCH", lambda: g.build_all(self.root, check_only=True))

    def test_export_fidelity_errors_are_explicit(self):
        for md, html, code in [("![extra](image.png)", "", "IMAGE_COVERAGE_MISMATCH"),
                               (MD.decode(), "<table></table>", "TABLE_COVERAGE_MISMATCH"),
                               (MD.decode(), "<iframe></iframe>", "UNSUPPORTED_EMBED"),
                               ("![x](https://example.com/x.png)", '<img src="x">', "UNFROZEN_IMAGE")]:
            self.expect_failure(code, lambda: g.convert(md, html))
        result, _, coverage = g.convert("![x](data:image/png;base64,iVBORw0KGgo=)", '<img src="https://docs.google.com/drawings/d/x/image">')
        self.assertIn(b"Image coverage warning", result)
        self.assertEqual(coverage["missing_image_occurrences"], 0)
        self.assertIn("has not been verified", coverage["image_gaps"][0])

    def test_symlinks_and_active_lock_are_rejected(self):
        (self.path.parent / "snapshots").symlink_to(self.repo, target_is_directory=True)
        with patch.object(g, "fetch", self.fetched):
            self.expect_failure("INVALID_PATH", lambda: g.build_all(self.root))
        with (self.path.parent / ".gdoc-import.lock").open("a+b") as stream:
            g.fcntl.flock(stream, g.fcntl.LOCK_EX | g.fcntl.LOCK_NB)
            self.expect_failure("IMPORT_LOCKED", lambda: g.build_all(self.root))

    def test_yaml_rejects_duplicate_keys_unsafe_tags_and_empty_revisions(self):
        for text, code in [('proposal: a\nproposal: b\nrevisions: {}', "INVALID_YAML"),
                           ('proposal: a\nrevisions: {rev1: {}, rev1: {}}', "INVALID_YAML"),
                           ('!!python/object:builtins.object {}', "INVALID_YAML"),
                           ('proposal: a\nrevisions: {}', "INVALID_PROPOSAL")]:
            self.path.write_text(text)
            self.expect_failure(code, lambda: g.build_all(self.root))

    def test_history_blocks_cleared_frozen_fields_and_removed_proposal(self):
        self.first()
        base = self.init_git()
        original = self.path.read_bytes()
        self.write({"proposal": "example", "revisions": {"rev1": {"google_doc_url": URL}}})
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.expect_failure("FROZEN_VERSION_CHANGED", lambda: g.build_all(self.root, base_ref=base))
            self.path.unlink()
            self.expect_failure("FROZEN_VERSION_CHANGED", lambda: g.build_all(self.root, base_ref=base))
        self.path.write_bytes(original)
        g.build_all(self.root, check_only=True, base_ref=base)

    def test_ci_baseline_allows_pending_commit_and_rejects_reset_in_new_commit(self):
        self.first()
        base = self.init_git()
        self.pending()
        self.commit()
        with patch.object(g, "fetch", self.newer):
            g.build_all(self.root, base_ref=base)
        self.assertEqual(g.load_proposal(self.path)["revisions"]["rev2"]["google_revision"], "13")
        self.write({"proposal": "example", "revisions": {"rev1": {"google_doc_url": URL}}})
        self.commit()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.expect_failure("FROZEN_VERSION_CHANGED", lambda: g.build_all(self.root, base_ref=base))

    def test_all_manifests_are_preflighted_before_any_capture(self):
        other = self.root / "z-invalid/manifest.yaml"
        other.parent.mkdir()
        other.write_text("proposal: broken\nrevisions: {rev1: {google_doc_url: invalid}}")
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.expect_failure("INVALID_URL", lambda: g.build_all(self.root))

    def test_cli_build_and_check_work_offline_for_frozen_versions(self):
        self.first()
        before = self.tree()
        for command in ("build", "check"):
            result = subprocess.run([sys.executable, g.__file__, command, str(self.root)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)[str(self.path)]["mode"], "offline")
        self.assertEqual(self.tree(), before)


if __name__ == "__main__":
    unittest.main()
