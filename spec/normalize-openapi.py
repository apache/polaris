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

closer = re.compile(r"^(\s*)([}\]])(,?)\s*$")
block_scalar = re.compile(r"^(\s*)\S.*:\s*[|>][0-9+-]*\s*(#.*)?$")
flow_start = re.compile(r"[{[]\s*(?:#.*)?$")


def normalize_text(text: str) -> str:
    lines = []
    scalar_indent = None
    flow_indents = []

    for line in text.splitlines(keepends=True):
        stripped = line.rstrip("\n")
        indent = len(stripped) - len(stripped.lstrip())

        if scalar_indent is not None:
            if stripped.strip() and indent <= scalar_indent:
                scalar_indent = None
            else:
                lines.append(line)
                continue

        match = closer.match(stripped)
        if match and flow_indents:
            open_indent = flow_indents.pop()
            if indent <= open_indent:
                stripped = " " + stripped

        lines.append(stripped + ("\n" if line.endswith("\n") else ""))

        if block_scalar.match(stripped):
            scalar_indent = indent
        elif flow_start.search(stripped):
            flow_indents.append(indent)

    return "".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix flow-collection indentation in a YAML spec so Redocly can bundle it."
    )
    parser.add_argument("path", help="YAML file to normalize")
    path = parser.parse_args().path

    with open(path, encoding="utf-8") as f:
        text = f.read()
    with open(path, "w", encoding="utf-8") as f:
        f.write(normalize_text(text))


if __name__ == "__main__":
    main()
