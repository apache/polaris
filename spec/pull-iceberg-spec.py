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
import argparse
import re
import subprocess
from pathlib import Path

REPO = "https://github.com/apache/iceberg.git"
RAW = "https://raw.githubusercontent.com/apache/iceberg"
SPEC_PATH = "open-api/rest-catalog-open-api.yaml"
ASF_HEADER = """#
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
#"""


def resolve_ref(kind: str, ref: str) -> str:
    if kind == "sha":
        return ref

    if kind == "tag":
        remote_ref = f"refs/tags/{ref}"
        result = subprocess.run(
            ["git", "ls-remote", "--tags", REPO, f"{remote_ref}*"],
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.splitlines():
            sha, name = line.split("\t", 1)
            if name == f"{remote_ref}^{{}}":
                return sha
        for line in result.stdout.splitlines():
            sha, name = line.split("\t", 1)
            if name == remote_ref:
                return sha
        raise SystemExit(f"No Iceberg tag named {ref!r}")

    remote_ref = f"refs/heads/{ref}"
    result = subprocess.run(
        ["git", "ls-remote", REPO, remote_ref],
        capture_output=True,
        text=True,
        check=True,
    )
    for line in result.stdout.splitlines():
        sha, name = line.split("\t", 1)
        if name == remote_ref:
            return sha
    raise SystemExit(f"No Iceberg branch named {ref!r}")


def fetch_spec(sha: str) -> str:
    url = f"{RAW}/{sha}/{SPEC_PATH}"
    result = subprocess.run(["curl", "-fsSL", url], capture_output=True, check=True)
    return result.stdout.decode("utf-8")


def render_metadata(kind: str, ref: str, sha: str) -> str:
    lines = [
        ASF_HEADER,
        "",
        "CODE_COPIED_TO_POLARIS",
        "Source: Apache Iceberg REST Catalog API",
    ]
    if kind == "tag":
        lines.append(f"Apache Iceberg Version: {ref.removeprefix('apache-iceberg-')}")
    elif kind == "branch":
        lines.append(f"Apache Iceberg Branch: {ref}")
    lines.extend(
        [
            f"Commit SHA: {sha}",
            f"Upstream: https://github.com/apache/iceberg/blob/{sha}/{SPEC_PATH}",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch the Iceberg REST catalog spec.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sha", help="Iceberg commit SHA")
    group.add_argument("--tag", help="Iceberg release tag")
    group.add_argument("--branch", help="Iceberg branch")
    parser.add_argument(
        "--output",
        default="spec/iceberg-rest-catalog-open-api.yaml",
        help="YAML file to write (default: spec/iceberg-rest-catalog-open-api.yaml)",
    )
    args = parser.parse_args()

    if args.sha is not None:
        kind, ref = "sha", args.sha
    elif args.tag is not None:
        kind, ref = "tag", args.tag
    else:
        kind, ref = "branch", args.branch

    if kind == "sha":
        if not re.fullmatch(r"[0-9a-fA-F]{40}", ref):
            parser.error(f"invalid SHA: {ref!r}")
    elif not re.fullmatch(r"[A-Za-z0-9._/-]+", ref):
        parser.error(f"invalid {kind}: {ref!r}")

    sha = resolve_ref(kind, ref)
    output = Path(args.output)
    metadata = output.with_suffix(".metadata.txt")
    output.write_text(fetch_spec(sha), encoding="utf-8")
    metadata.write_text(render_metadata(kind, ref, sha), encoding="utf-8")
    print(f"Wrote {output} and {metadata} from Iceberg {kind} {ref} ({sha})")


if __name__ == "__main__":
    main()
