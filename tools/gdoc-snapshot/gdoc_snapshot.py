#!/usr/bin/env python3
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

"""PROTOTYPE: anonymously freeze a public Google Doc; check snapshots offline.

Python 3.10+, PyYAML. This is not an official Google API client.
"""
import argparse
import base64
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import re
import shutil
import sys
import subprocess

import yaml
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path

VERSION = "0.6.0"
MAX_BYTES = 32 * 1024 * 1024


class Failure(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def fail(code, message):
    raise Failure(code, message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def read_json(path):
    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                fail("INVALID_JSON", f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(path.read_bytes(), object_pairs_hook=unique_keys)
    except (OSError, ValueError) as e:
        fail("INVALID_JSON", f"{path}: {e}")
    if not isinstance(value, dict):
        fail("INVALID_JSON", f"{path}: expected a JSON object.")
    return value


def parse_url(url):
    if not isinstance(url, str):
        fail("INVALID_URL", "Expected a Google Docs URL.")
    u = urllib.parse.urlsplit(url)
    m = re.fullmatch(r"/document/d/([A-Za-z0-9_-]+)/(edit|export|view|preview)", u.path)
    if u.scheme != "https" or u.netloc != "docs.google.com" or not m:
        fail("INVALID_URL", "Expected https://docs.google.com/document/d/<id>/edit or /export.")
    q = urllib.parse.parse_qs(u.query, keep_blank_values=True)
    f = urllib.parse.parse_qs(u.fragment, keep_blank_values=True)
    version_keys = {"rev", "revisionid", "version", "versionid", "history", "namedversion"}
    if any(k.lower() in version_keys or (k.lower() == "revision" and k != "revision") for k in set(q) | set(f)):
        fail("UNSUPPORTED_VERSION_LINK", "Only the verified numeric ?revision= form is supported.")
    if "revision" in f:
        fail("UNSUPPORTED_VERSION_LINK", "A fragment revision is not a verified download selector.")
    versions = q.get("revision", [])
    if versions and (len(versions) != 1 or not re.fullmatch(r"[1-9][0-9]*", versions[0])):
        fail("INVALID_REVISION", "revision must be one positive numeric ID.")
    return m[1], versions[0] if versions else None


def download_url(doc_id, revision, fmt="md"):
    url = f"https://docs.google.com/document/d/{doc_id}/export?format={fmt}"
    return url if revision is None else f"{url}&revision={revision}"


def validate_host(url):
    u = urllib.parse.urlsplit(url)
    host = u.hostname or ""
    if u.scheme != "https" or u.username or u.password or u.port not in (None, 443):
        fail("UNEXPECTED_REDIRECT", "Only HTTPS Google document downloads are accepted.")
    if host == "accounts.google.com":
        fail("LOGIN_REQUIRED", "Google redirected this anonymous request to sign-in.")
    if host != "docs.google.com" and host != "googleusercontent.com" and not host.endswith(".googleusercontent.com"):
        fail("UNEXPECTED_REDIRECT", f"Unexpected download host: {host}")


class PublicRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_host(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def fetch(url, kind):
    validate_host(url)
    # No CookieJar, Cookie, Authorization, browser session, or Google credentials.
    opener = urllib.request.build_opener(PublicRedirect())
    request = urllib.request.Request(url, headers={"User-Agent": f"gdoc-snapshot-prototype/{VERSION}"})
    try:
        with opener.open(request, timeout=45) as response:
            content_type = response.headers.get_content_type()
            data = response.read(MAX_BYTES + 1)
    except urllib.error.HTTPError as e:
        fail("ANONYMOUS_FETCH_FAILED", f"{kind}: HTTP {e.code}. Check public access, revision availability, and the web endpoint. No latest-version fallback.")
    except (urllib.error.URLError, TimeoutError) as e:
        fail("NETWORK_ERROR", f"{kind}: {e}")
    if len(data) > MAX_BYTES:
        fail("TOO_LARGE", f"{kind} exceeds this prototype's 32 MiB limit.")
    allowed = {"md": {"text/markdown", "text/x-markdown"}, "html": {"text/html"}, "page": {"text/html"}}
    if content_type not in allowed[kind]:
        fail("UNEXPECTED_CONTENT", f"{kind}: got {content_type}, not document content.")
    return data


def resolve_revision(page):
    # This is an observed Google web format, not a stable public API.
    # Scope discovery to scripts so proposal text cannot supply the selector.
    scripts = re.findall(r"<script\b[^>]*>(.*?)</script>", page, re.S | re.I)
    starts, chunks = [], set()
    for script in scripts:
        starts += re.findall(r"(?:^|;)\s*DOCS_warmStartDocumentLoader\.startLoad\(\s*(\d+)(?:\.0)?\s*,", script)
        if re.search(r"\bDOCS_modelChunk\s*=", script):
            chunks.update(re.findall(r'"revision"\s*:\s*(\d+)\s*}', script))
    if len(starts) != 1 or not chunks or chunks != {starts[0]} or not re.fullmatch(r"[1-9][0-9]*", starts[0]):
        fail("REVISION_DISCOVERY_FAILED", "The anonymous page did not expose one consistent revision in the expected loader and model fields. No unversioned export was attempted.")
    return starts[0]


class HtmlCoverage(HTMLParser):
    def __init__(self):
        super().__init__()
        self.images = []
        self.tables = 0
        self.rich = []

    def handle_starttag(self, tag, attrs):
        if tag == "img":
            self.images.append(dict(attrs).get("src", ""))
        if tag == "table":
            self.tables += 1
        if tag in ("iframe", "object", "embed", "svg", "video", "audio", "canvas"):
            self.rich.append(tag)


DATA_IMAGE = re.compile(r"data:image/(png|jpeg|gif|webp);base64,([A-Za-z0-9+/=\r\n]+)")
REF_DEF = re.compile(r"(?m)^\[([^\]\n]+)\]:\s*(?:<([^>]+)>|(\S+))\s*$")
REF_IMAGE = re.compile(r"!\[[^\]\n]*\]\[([^\]\n]+)\]")
INLINE_IMAGE = re.compile(r"!\[[^\]\n]*\]\(([^)\n]+)\)")


def convert(md, html):
    """Conservative image/table coverage check, not a full semantic equivalence proof."""
    coverage = HtmlCoverage()
    coverage.feed(html)
    if coverage.rich:
        fail("UNSUPPORTED_EMBED", f"HTML contains unsupported elements: {sorted(set(coverage.rich))}")
    drawing_count = sum("docs.google.com/drawings/" in src for src in coverage.images)
    if re.search(r"<(?:img|iframe|object|svg|video|audio)\b", md, re.I):
        fail("UNSUPPORTED_EMBED", "Raw embedded HTML in Markdown needs an explicit converter.")
    definitions = {m[1]: (m[2] or m[3]) for m in REF_DEF.finditer(md)}
    refs = REF_IMAGE.findall(md)
    inline = INLINE_IMAGE.findall(md)
    sources = []
    for ref in refs:
        if ref not in definitions:
            fail("MISSING_IMAGE", f"Undefined image reference: {ref}")
        sources.append(definitions[ref])
    sources += inline
    if len(sources) != md.count("!["):
        fail("UNSUPPORTED_IMAGE_SYNTAX", "Some Markdown image syntax could not be parsed safely.")
    image_gaps = []
    if len(sources) > len(coverage.images):
        fail("IMAGE_COVERAGE_MISMATCH", "Markdown has more image occurrences than HTML. The coverage check cannot account for this export discrepancy.")
    missing_count = len(coverage.images) - len(sources)
    if missing_count:
        image_gaps.append(f"HTML contains {len(coverage.images)} image occurrences, but Markdown contains {len(sources)}. {missing_count} image occurrences are missing from this Markdown snapshot.")
    if drawing_count:
        image_gaps.append(f"HTML contains {drawing_count} Google Drawing occurrences. Their representation in Markdown has not been verified. Exact locations in Markdown are unknown.")
    tables = len(re.findall(r"(?m)^\|(?:\s*:?-{3,}:?\s*\|)+\s*$", md))
    if tables != coverage.tables:
        fail("TABLE_COVERAGE_MISMATCH", f"HTML has {coverage.tables} tables but Markdown has {tables}.")
    assets, replacements = {}, {}
    for src in sources:
        m = DATA_IMAGE.fullmatch(src)
        if not m:
            fail("UNFROZEN_IMAGE", "Only embedded PNG/JPEG/GIF/WebP images are supported. External images cannot remain in the snapshot.")
        try:
            raw = base64.b64decode(re.sub(r"\s", "", m[2]), validate=True)
        except ValueError:
            fail("INVALID_IMAGE", "Invalid base64 image.")
        signatures = {"png": b"\x89PNG\r\n\x1a\n", "jpeg": b"\xff\xd8\xff", "gif": b"GIF", "webp": b"RIFF"}
        if not raw.startswith(signatures[m[1]]) or (m[1] == "webp" and raw[8:12] != b"WEBP"):
            fail("INVALID_IMAGE", "Image bytes do not match the declared format.")
        path = f"assets/{digest(raw)}.{m[1]}"
        assets[path] = raw
        replacements[src] = path
    result = md
    for old, new in replacements.items():
        result = result.replace(old, new)
    if "data:image/" in result:
        fail("UNHANDLED_IMAGE", "An image was not associated with a supported Markdown reference.")
    # Preserve Google's text and code formatting. Do not silently rewrite examples.
    warnings = ["Image/table coverage is not a full semantic or visual fidelity proof."]
    if image_gaps:
        warnings.extend(image_gaps)
        notice = ["**Image coverage warning**", *image_gaps,
                  "This snapshot may omit information carried by images. Do not infer the contents of missing images from this Markdown. No textual replacement was generated."]
        result = "\n".join("> " + line for line in notice) + "\n\n" + result
    if "\\`\\`\\`" in md:
        warnings.append("Google exported some triple backticks as literal text. Review code examples.")
    return result.encode(), assets, {"html_images": len(coverage.images), "markdown_images": len(sources), "html_drawings": drawing_count, "missing_image_occurrences": missing_count, "image_gaps": image_gaps, "html_tables": coverage.tables, "markdown_tables": tables, "warnings": warnings}


def safe_child(root, relative):
    if not isinstance(relative, str) or not relative or "\\" in relative or Path(relative).is_absolute() or any(x in ("", "..", ".") for x in relative.split("/")):
        fail("INVALID_PATH", "Snapshot paths must be relative and contained in the proposal directory.")
    path = root
    for part in relative.split("/"):
        path = path / part
        if path.is_symlink():
            fail("INVALID_PATH", "Snapshot paths cannot be symlinks.")
    return path


def validate_bundle(bundle):
    manifest_path = safe_child(bundle, "manifest.json")
    manifest = read_json(manifest_path)
    if not isinstance(manifest, dict) or manifest.get("format") != 1 or not isinstance(manifest.get("files"), dict):
        fail("INVALID_MANIFEST", "Unknown snapshot manifest format.")
    if not {"document.md", "source.md"} <= set(manifest["files"]):
        fail("INVALID_MANIFEST", "Snapshot must contain document.md and source.md.")
    if bundle.name != digest(manifest_path.read_bytes()):
        fail("MANIFEST_HASH_MISMATCH", "The snapshot directory name does not match its manifest hash.")
    for name, expected in manifest["files"].items():
        f = safe_child(bundle, name)
        if not f.is_file() or digest(f.read_bytes()) != expected:
            fail("FILE_HASH_MISMATCH", f"Missing or modified snapshot file: {name}")
    actual = {str(f.relative_to(bundle)) for f in bundle.rglob("*") if f.is_file() or f.is_symlink()}
    if actual != set(manifest["files"]) | {"manifest.json"}:
        fail("UNEXPECTED_FILES", "Snapshot contains files not recorded in its manifest.")
    return manifest


def validate_source(bundle_manifest, doc_id, revision):
    source = bundle_manifest.get("source", {})
    if revision is None:
        fail("UNPINNED_SOURCE", "No recorded Google revision. Import into a new proposal version; do not relabel old bytes.")
    if (source.get("document_id"), source.get("revision")) != (doc_id, revision):
        fail("SOURCE_MISMATCH", "Google document/revision do not match the snapshot.")
    if source.get("download_url") != download_url(doc_id, revision):
        fail("SOURCE_MISMATCH", "Snapshot export URL does not select its recorded Google revision.")
    if source.get("capture_mode", "revision-export") != "revision-export":
        fail("SOURCE_MISMATCH", "Snapshot was not exported at a fixed Google revision.")
    if parse_url(source.get("original_url"))[0] != doc_id:
        fail("SOURCE_MISMATCH", "Snapshot original URL refers to a different document.")


def validate_version(version):
    if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", version):
        fail("INVALID_VERSION", "Use a proposal version such as rev1 (letters, digits, dot, underscore or hyphen).")


class UniqueSafeLoader(yaml.SafeLoader):
    """Reject duplicate keys rather than silently losing a proposal version."""


def yaml_mapping(loader, node):
    loader.flatten_mapping(node)
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        if not isinstance(key, str) or key in result:
            fail("INVALID_YAML", "Manifest keys must be unique strings.")
        result[key] = loader.construct_object(value_node)
    return result


UniqueSafeLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, yaml_mapping)
GENERATED_FIELDS = {"google_revision", "snapshot_md", "snapshot_manifest"}
YAML_HEADER = "\n".join(line for line in Path(__file__).read_text().splitlines()[1:19]) + "\n\n"


def parse_proposal(data):
    try:
        proposal = yaml.load(data, Loader=UniqueSafeLoader)
    except yaml.YAMLError as e:
        fail("INVALID_YAML", str(e))
    if (not isinstance(proposal, dict) or set(proposal) != {"proposal", "revisions"}
            or not isinstance(proposal["proposal"], str) or not proposal["proposal"].strip()
            or not isinstance(proposal["revisions"], dict) or not proposal["revisions"]):
        fail("INVALID_PROPOSAL", "Expected proposal: <name> and a nonempty revisions mapping.")
    for version, entry in proposal["revisions"].items():
        validate_version(version)
        if not isinstance(entry, dict) or "google_doc_url" not in entry:
            fail("INVALID_PROPOSAL", f"{version}: expected google_doc_url.")
        if set(entry) not in ({"google_doc_url"}, {"google_doc_url"} | GENERATED_FIELDS):
            fail("PARTIAL_ENTRY", f"{version}: supply only google_doc_url, or retain all generated fields. Partial or unknown fields are not accepted.")
        _, revision = parse_url(entry["google_doc_url"])
        if revision is not None:
            fail("INVALID_URL", "Use the ordinary Google Doc link. Google revisions are recorded by the build.")
    return proposal


def load_proposal(path):
    return parse_proposal(path.read_bytes())


def proposal_bytes(proposal):
    return (YAML_HEADER + yaml.safe_dump(proposal, allow_unicode=True, sort_keys=False, width=120)).encode()


def check_entry(root, version, entry):
    validate_version(version)
    if not isinstance(entry, dict):
        fail("INVALID_PROPOSAL", "Each proposal version must be an object.")
    doc_id, url_revision = parse_url(entry.get("google_doc_url"))
    if url_revision is not None:
        fail("SOURCE_MISMATCH", "google_doc_url must be the ordinary document link; use google_revision for the version.")
    revision = entry.get("google_revision")
    if not isinstance(revision, str) or not re.fullmatch(r"[1-9][0-9]*", revision):
        fail("INVALID_REVISION", "google_revision must be a positive numeric string.")
    manifest_path = entry.get("snapshot_manifest")
    if not isinstance(manifest_path, str) or not re.fullmatch(r"snapshots/[0-9a-f]{64}/manifest.json", manifest_path):
        fail("INVALID_PATH", "snapshot_manifest must point to snapshots/<hash>/manifest.json.")
    bundle_path = safe_child(root, manifest_path).parent
    bundle = validate_bundle(bundle_path)
    validate_source(bundle, doc_id, revision)
    expected_md = [str(bundle_path.relative_to(root) / "document.md")]
    if entry.get("snapshot_md") != expected_md:
        fail("SNAPSHOT_PATH_MISMATCH", "snapshot_md must name the verified document.md in this snapshot. This exporter produces one Markdown file for the whole document.")
    return {"google_revision": revision, "snapshot_md": expected_md,
            "files": len(bundle["files"]), "warnings": bundle.get("coverage", {}).get("warnings", [])}


def check_proposal(path, proposal, allow_pending=False):
    results = {}
    for version, entry in proposal["revisions"].items():
        if not GENERATED_FIELDS.intersection(entry):
            if not allow_pending:
                fail("PENDING_SNAPSHOT", f"{version}: snapshot is pending. Run make proposal-snapshots or allow branch CI to capture it.")
            results[version] = {"status": "pending"}
        else:
            results[version] = check_entry(path.parent, version, entry)
    return results


def check(path):
    proposal = load_proposal(path)
    return {"status": "ok", "mode": "offline", "proposal": proposal["proposal"],
            "revisions": check_proposal(path, proposal)}


def sync_file(path, data):
    with path.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def proposal_lock(path):
    if path.is_symlink():
        fail("INVALID_PATH", "The proposal manifest cannot be a symlink.")
    with safe_child(path.parent, ".gdoc-import.lock").open("a+b") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fail("IMPORT_LOCKED", "Another import is active for this proposal.")
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def publish_proposal(path, original_bytes, proposal):
    # Validate all old and new entries before the only publication point.
    check_proposal(path, proposal)
    fd, name = tempfile.mkstemp(prefix=".gdoc-manifest-", dir=path.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        sync_file(temporary, proposal_bytes(proposal))
        if path.is_symlink() or path.read_bytes() != original_bytes:
            fail("SOURCE_CHANGED", "Proposal manifest changed during import; it was not overwritten.")
        os.replace(temporary, path)
        sync_dir(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def archive_bundle(root, doc_id, revision, original_url):
    export_url = download_url(doc_id, revision)
    raw_md = fetch(export_url, "md")
    raw_html = fetch(download_url(doc_id, revision, "html"), "html")
    markdown, assets, coverage = convert(raw_md.decode("utf-8"), raw_html.decode("utf-8"))
    coverage["comparison_scope"] = "same-revision-exports"
    files = {"document.md": markdown, "source.md": raw_md, **assets}
    manifest = {
        "format": 1,
        "tool": {"version": VERSION, "sha256": digest(Path(__file__).read_bytes())},
        "source": {"document_id": doc_id, "revision": revision, "capture_mode": "revision-export",
                   "original_url": original_url, "download_url": export_url},
        "coverage": coverage,
        "files": {name: digest(data) for name, data in files.items()},
    }
    manifest_bytes = canonical(manifest)
    bundle_id = digest(manifest_bytes)
    snapshots = safe_child(root, "snapshots")
    snapshots.mkdir(exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".gdoc-stage-", dir=snapshots))
    try:
        for name, data in {**files, "manifest.json": manifest_bytes}.items():
            target = stage / name
            target.parent.mkdir(parents=True, exist_ok=True)
            sync_file(target, data)
        for directory in sorted((p for p in stage.rglob("*") if p.is_dir()), reverse=True):
            sync_dir(directory)
        sync_dir(stage)
        final = safe_child(snapshots, bundle_id)
        if final.exists():
            validate_bundle(final)
        else:
            os.rename(stage, final)
            sync_dir(snapshots)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return f"snapshots/{bundle_id}"


def make_entry(doc_id, revision, pointer):
    return {"google_doc_url": f"https://docs.google.com/document/d/{doc_id}/edit",
            "google_revision": revision, "snapshot_md": [f"{pointer}/document.md"],
            "snapshot_manifest": f"{pointer}/manifest.json"}


def build_manifest(path):
    """Fill all pending versions, publishing this manifest only after all succeed."""
    with proposal_lock(path):
        original_bytes = path.read_bytes()
        proposal = parse_proposal(original_bytes)
        checked = check_proposal(path, proposal, allow_pending=True)
        pending = [version for version, result in checked.items() if result.get("status") == "pending"]
        if not pending:
            return {"status": "unchanged", "mode": "offline", "revisions": checked}
        entries = proposal["revisions"]
        selected_revisions = {}
        for version in pending:
            url = entries[version]["google_doc_url"]
            doc_id, _ = parse_url(url)
            if doc_id not in selected_revisions:
                page = fetch(f"https://docs.google.com/document/d/{doc_id}/edit", "page").decode("utf-8")
                selected_revisions[doc_id] = resolve_revision(page)
            selected = selected_revisions[doc_id]
            match = next((entry for entry in entries.values()
                          if parse_url(entry["google_doc_url"])[0] == doc_id
                          and entry.get("google_revision") == selected), None)
            if match:
                entry = dict(match)
                entry["snapshot_md"] = list(match["snapshot_md"])
            else:
                pointer = archive_bundle(path.parent, doc_id, selected, url)
                entry = make_entry(doc_id, selected, pointer)
            entry["google_doc_url"] = url
            entries[version] = entry
        publish_proposal(path, original_bytes, proposal)
        return {**check(path), "status": "generated", "mode": "capture", "versions": pending}


def git_result(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=False)


def protect_history(root, proposals, base_ref):
    """A cleared frozen entry must never become a new capture request."""
    found = git_result(root, "rev-parse", "--show-toplevel")
    if found.returncode:
        if base_ref:
            fail("INVALID_BASE", "--base-ref requires a Git repository.")
        return
    repo = Path(os.fsdecode(found.stdout).strip())
    ref = base_ref or "HEAD"
    if git_result(repo, "rev-parse", "--verify", ref + "^{commit}").returncode:
        fail("INVALID_BASE", f"Unknown base commit: {ref}")
    prefix = root.resolve().relative_to(repo.resolve()).as_posix()
    listing = git_result(repo, "ls-tree", "-r", "--name-only", ref, "--", prefix)
    for name in os.fsdecode(listing.stdout).splitlines():
        path = repo / name
        if path.name != "manifest.yaml" or path.parent.parent.resolve() != root.resolve():
            continue
        previous = parse_proposal(git_result(repo, "show", f"{ref}:{name}").stdout)
        frozen = {version: entry for version, entry in previous["revisions"].items()
                  if GENERATED_FIELDS.intersection(entry)}
        if not frozen:
            continue
        current = proposals.get(path.resolve())
        if current is None or current["proposal"] != previous["proposal"]:
            fail("FROZEN_VERSION_CHANGED", f"Archived proposal removed or renamed: {name}")
        for version, entry in frozen.items():
            if current["revisions"].get(version) != entry:
                fail("FROZEN_VERSION_CHANGED", f"{name}: {version} is already archived. Add a new version instead.")


def build_all(root, check_only=False, base_ref=None):
    if not root.is_dir() or root.is_symlink():
        fail("INVALID_PATH", "Expected a real proposals directory.")
    paths = sorted(root.glob("*/manifest.yaml"))
    proposals = {}
    # Preflight every manifest and old bundle before any network or publication.
    for path in paths:
        if path.is_symlink() or path.parent.is_symlink():
            fail("INVALID_PATH", "Proposal manifests and directories cannot be symlinks.")
        proposal = load_proposal(path)
        check_proposal(path, proposal, allow_pending=not check_only)
        proposals[path.resolve()] = proposal
    protect_history(root, proposals, base_ref)
    return {str(path): check(path) if check_only else build_manifest(path) for path in paths}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "check"))
    parser.add_argument("directory", nargs="?", type=Path, default=Path("proposals"))
    parser.add_argument("--base-ref", help="Git commit whose frozen entries must remain unchanged; defaults to HEAD")
    args = parser.parse_args()
    try:
        result = build_all(args.directory, args.command == "check", args.base_ref)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Failure as e:
        print(json.dumps({"status": "error", "code": e.code, "message": str(e)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(json.dumps({"status": "error", "code": "INVALID_INPUT_OR_IO", "message": str(e)}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
