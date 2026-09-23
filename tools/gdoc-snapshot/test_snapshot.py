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

"""Behavior checks using controlled exports, never a live Google account."""
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import gdoc_snapshot as g

DOC = "example_doc"
URL = f"https://docs.google.com/document/d/{DOC}/edit"
PAGE = b'<script>DOCS_warmStartDocumentLoader.startLoad(12.0,"anonymous");</script><script>DOCS_modelChunk = {"revision":12};</script>'
MD = b"# Example\n\nFrozen text.\n"
HTML = b"<html><body><h1>Example</h1><p>Frozen text.</p></body></html>"


class SnapshotContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "manifest.json"
        self.path.write_bytes(g.canonical({"format": 1, "proposal": "example", "revisions": {}}))

    def tearDown(self):
        self.temp.cleanup()

    def fetched(self, url, kind):
        if kind != "page":
            self.assertIn("revision=12", url)
        return {"page": PAGE, "md": MD, "html": HTML}[kind]

    def initial_import(self):
        with patch.object(g, "fetch", self.fetched):
            return g.import_doc(self.path, "rev1", URL)

    def tree(self):
        return {str(p.relative_to(self.root)): p.read_bytes()
                for p in self.root.rglob("*") if p.is_file() and p.name != ".gdoc-import.lock"}

    def newer(self, url, kind):
        if kind != "page":
            self.assertIn("revision=13", url)
        return {"page": PAGE.replace(b"12", b"13"), "md": b"# New content\n", "html": b"<h1>New content</h1>"}[kind]

    def test_two_versions_preserve_old_entry_bytes_and_read_offline(self):
        first = self.initial_import()
        before = self.tree()
        entry = json.loads(self.path.read_bytes())["revisions"]["rev1"]
        with patch.object(g, "fetch", self.newer):
            second = g.import_doc(self.path, "rev2")
        proposal = json.loads(self.path.read_bytes())
        self.assertEqual(proposal["revisions"]["rev1"], entry)
        self.assertEqual(entry["google_doc_url"], URL)
        self.assertEqual(first["google_revision"], "12")
        self.assertEqual(second["google_revision"], "13")
        for path, data in before.items():
            if path != "manifest.json":
                self.assertEqual((self.root / path).read_bytes(), data)
        self.assertIn(b"New content", (self.root / second["snapshot_md"][0]).read_bytes())
        frozen = self.tree()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.assertEqual(set(g.check(self.path)["revisions"]), {"rev1", "rev2"})
            self.assertEqual(g.import_doc(self.path, "rev1")["status"], "unchanged")
            self.assertEqual(g.import_doc(self.path, "rev2")["status"], "unchanged")
        self.assertEqual(self.tree(), frozen)

    def test_existing_label_rejects_new_revision_or_document_before_network(self):
        self.initial_import()
        before = self.tree()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            for kwargs in ({"revision": "13"}, {"url": URL.replace(DOC, "different")}, {"url": g.download_url(DOC, "13")}):
                with self.assertRaises(g.Failure) as raised:
                    g.import_doc(self.path, "rev1", **kwargs)
                self.assertEqual(raised.exception.code, "VERSION_EXISTS")
        self.assertEqual(self.tree(), before)

    def test_explicit_revision_is_used_for_every_export_without_page_read(self):
        calls = []
        def fetched(url, kind):
            calls.append(kind)
            self.assertIn("revision=12", url)
            return {"md": MD, "html": HTML}[kind]
        with patch.object(g, "fetch", fetched):
            result = g.import_doc(self.path, "rev1", URL, "12")
        self.assertEqual(calls, ["md", "html"])
        self.assertEqual(result["google_revision"], "12")
        before = self.tree()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            with self.assertRaises(g.Failure) as raised:
                g.import_doc(self.path, "rev2", g.download_url(DOC, "12"), "13")
        self.assertEqual(raised.exception.code, "REVISION_CONFLICT")
        self.assertEqual(self.tree(), before)

    def test_same_google_revision_reuses_snapshot_for_another_label(self):
        first = self.initial_import()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            second = g.import_doc(self.path, "rev2", revision="12")
        self.assertTrue(second["reused_snapshot"])
        self.assertEqual(first["snapshot_md"], second["snapshot_md"])
        self.assertEqual(len(list((self.root / "snapshots").iterdir())), 1)

    def test_discovery_or_download_failure_never_publishes_or_falls_back(self):
        self.initial_import()
        before = self.tree()
        for page in (b"<html>new Google layout</html>", PAGE.replace(b'"revision":12', b'"revision":13')):
            calls = []
            def fetched(url, kind):
                calls.append(kind)
                return page
            with patch.object(g, "fetch", fetched):
                with self.assertRaises(g.Failure) as raised:
                    g.import_doc(self.path, "rev2")
            self.assertEqual(raised.exception.code, "REVISION_DISCOVERY_FAILED")
            self.assertEqual(calls, ["page"])
        calls = []
        def failed_export(url, kind):
            calls.append((url, kind))
            raise g.Failure("ANONYMOUS_FETCH_FAILED", "HTTP 400")
        with patch.object(g, "fetch", failed_export):
            with self.assertRaises(g.Failure):
                g.import_doc(self.path, "rev2", revision="13")
        self.assertEqual(calls, [(g.download_url(DOC, "13"), "md")])
        self.assertEqual(self.tree(), before)

    def test_conversion_failure_preserves_history_and_releases_lock(self):
        self.initial_import()
        before = self.tree()
        with patch.object(g, "fetch", side_effect=lambda url, kind: MD if kind == "md" else b"<table></table>"):
            with self.assertRaises(g.Failure) as raised:
                g.import_doc(self.path, "rev2", revision="13")
        self.assertEqual(raised.exception.code, "TABLE_COVERAGE_MISMATCH")
        self.assertEqual(self.tree(), before)
        with (self.root / ".gdoc-import.lock").open("a+b") as stream:
            g.fcntl.flock(stream, g.fcntl.LOCK_EX | g.fcntl.LOCK_NB)

    def test_interrupted_manifest_publish_preserves_valid_old_history(self):
        self.initial_import()
        before = self.path.read_bytes()
        with patch.object(g, "fetch", self.newer), patch.object(g.os, "replace", side_effect=OSError("publication failure")):
            with self.assertRaises(OSError):
                g.import_doc(self.path, "rev2")
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(set(g.check(self.path)["revisions"]), {"rev1"})
        self.assertEqual(list(self.root.glob(".gdoc-manifest-*")), [])
        self.assertEqual(list((self.root / "snapshots").glob(".gdoc-stage-*")), [])

    def test_concurrent_manual_manifest_edit_is_not_overwritten(self):
        self.initial_import()
        user_bytes = self.path.read_bytes().replace(b'"example"', b'"user-edit"')
        def content(url, kind):
            self.path.write_bytes(user_bytes)
            return self.newer(url, kind)
        with patch.object(g, "fetch", content):
            with self.assertRaises(g.Failure) as raised:
                g.import_doc(self.path, "rev2")
        self.assertEqual(raised.exception.code, "SOURCE_CHANGED")
        self.assertEqual(self.path.read_bytes(), user_bytes)

    def test_modified_old_snapshot_blocks_check_and_new_import(self):
        first = self.initial_import()
        (self.root / first["snapshot_md"][0]).write_text("changed")
        before = self.path.read_bytes()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            for operation in (lambda: g.check(self.path), lambda: g.import_doc(self.path, "rev2")):
                with self.assertRaises(g.Failure) as raised:
                    operation()
                self.assertEqual(raised.exception.code, "FILE_HASH_MISMATCH")
        self.assertEqual(self.path.read_bytes(), before)

    def test_forged_source_metadata_and_markdown_path_are_rejected(self):
        self.initial_import()
        original = json.loads(self.path.read_bytes())
        for key, value, code in [("google_revision", "13", "SOURCE_MISMATCH"),
                                 ("google_doc_url", URL.replace(DOC, "different"), "SOURCE_MISMATCH"),
                                 ("snapshot_md", ["../../outside.md"], "SNAPSHOT_PATH_MISMATCH"),
                                 ("snapshot_manifest", "../manifest.json", "INVALID_PATH")]:
            changed = json.loads(json.dumps(original))
            changed["revisions"]["rev1"][key] = value
            self.path.write_bytes(g.canonical(changed))
            with self.assertRaises(g.Failure) as raised:
                g.check(self.path)
            self.assertEqual(raised.exception.code, code)

    def test_missing_images_warn_and_embedded_images_are_local_and_hashed(self):
        md = b"![image](data:image/png;base64,iVBORw0KGgo=)\n"
        html = b'<img src="inline"><img src="https://docs.google.com/drawings/d/example/image">'
        with patch.object(g, "fetch", side_effect=lambda url, kind: {"page": PAGE, "md": md, "html": html}[kind]):
            result = g.import_doc(self.path, "rev1", URL)
        document = self.root / result["snapshot_md"][0]
        self.assertTrue(document.read_text().startswith("> **Image coverage warning**"))
        self.assertIn("1 image occurrences are missing", document.read_text())
        self.assertIn("assets/", document.read_text())
        self.assertNotIn("data:image/", document.read_text())
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            self.assertEqual(g.check(self.path)["revisions"]["rev1"]["warnings"], result["warnings"])
        asset = next((document.parent / "assets").iterdir())
        asset.write_bytes(b"changed")
        with self.assertRaises(g.Failure) as raised:
            g.check(self.path)
        self.assertEqual(raised.exception.code, "FILE_HASH_MISMATCH")

    def test_image_surplus_tables_and_unsupported_embeds_still_fail(self):
        for md, html, code in [("![extra](image.png)", "", "IMAGE_COVERAGE_MISMATCH"),
                               (MD.decode(), "<table></table>", "TABLE_COVERAGE_MISMATCH"),
                               (MD.decode(), "<iframe></iframe>", "UNSUPPORTED_EMBED")]:
            with self.assertRaises(g.Failure) as raised:
                g.convert(md, html)
            self.assertEqual(raised.exception.code, code)
        output, _, coverage = g.convert("![x](data:image/png;base64,iVBORw0KGgo=)", '<img src="https://docs.google.com/drawings/d/x/image">')
        self.assertIn(b"Image coverage warning", output)
        self.assertEqual(coverage["missing_image_occurrences"], 0)
        self.assertIn("has not been verified", coverage["image_gaps"][0])

    def test_invalid_selectors_paths_and_active_lock_are_rejected(self):
        for suffix in ("?revisionId=12", "#revision=12", "#history=12", "?version=12", "?REVISION=12"):
            with self.assertRaises(g.Failure):
                g.parse_url(URL + suffix)
        with self.assertRaises(g.Failure):
            g.import_doc(self.path, "../rev1", URL)
        (self.root / "snapshots").symlink_to(self.root, target_is_directory=True)
        with patch.object(g, "fetch", self.fetched):
            with self.assertRaises(g.Failure) as raised:
                g.import_doc(self.path, "rev1", URL)
        self.assertEqual(raised.exception.code, "INVALID_PATH")
        with (self.root / ".gdoc-import.lock").open("a+b") as stream:
            g.fcntl.flock(stream, g.fcntl.LOCK_EX | g.fcntl.LOCK_NB)
            with self.assertRaises(g.Failure) as raised:
                g.import_doc(self.path, "rev1", URL)
            self.assertEqual(raised.exception.code, "IMPORT_LOCKED")

    def test_legacy_migration_is_offline_and_preserves_every_snapshot_byte(self):
        legacy = self.root / "legacy"
        shutil.copytree(Path(__file__).parent / "fixtures/pinned", legacy,
                        ignore=shutil.ignore_patterns(".gdoc-*"))
        path = legacy / "manifest.json"
        path.write_bytes(g.canonical({"format": 1, "proposal": "tag", "revisions": {}}))
        before = self.tree()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            result = g.migrate(legacy / "source.json", path, "rev1")
            self.assertEqual(result["google_revision"], "12")
            self.assertEqual(g.migrate(legacy / "source.json", path, "rev1")["status"], "unchanged")
            self.assertEqual(g.check(path)["status"], "ok")
        for name, data in before.items():
            if name != "legacy/manifest.json":
                self.assertEqual((self.root / name).read_bytes(), data)

    def test_unpinned_legacy_cannot_be_relabelled_with_a_google_revision(self):
        legacy = self.root / "legacy"
        shutil.copytree(Path(__file__).parent / "fixtures/current", legacy,
                        ignore=shutil.ignore_patterns(".gdoc-*"))
        path = legacy / "manifest.json"
        path.write_bytes(self.path.read_bytes())
        before = self.tree()
        with patch.object(g, "fetch", side_effect=AssertionError("NETWORK USED")):
            with self.assertRaises(g.Failure) as raised:
                g.migrate(legacy / "source.json", path, "rev1")
        self.assertEqual(raised.exception.code, "UNPINNED_SOURCE")
        self.assertEqual(self.tree(), before)

    def test_cli_checks_manifest_and_rejects_legacy_latest_option(self):
        self.initial_import()
        script = str(Path(g.__file__).resolve())
        check = subprocess.run([sys.executable, script, "check", str(self.path)], capture_output=True, text=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        self.assertEqual(json.loads(check.stdout)["mode"], "offline")
        before = self.tree()
        old_cli = subprocess.run([sys.executable, script, "import", str(self.path), "--version", "rev1", "--latest"], capture_output=True, text=True)
        self.assertNotEqual(old_cli.returncode, 0)
        self.assertEqual(self.tree(), before)

    def test_duplicate_version_keys_are_not_silently_accepted(self):
        self.path.write_text('{"format":1,"proposal":"example","revisions":{"rev1":{},"rev1":{}}}')
        with self.assertRaises(g.Failure) as raised:
            g.check(self.path)
        self.assertEqual(raised.exception.code, "INVALID_JSON")


if __name__ == "__main__":
    unittest.main()
