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

import re
from io import TextIOBase
from typing import List, Optional

docker_compose_up_pattern = re.compile("\\s*(\\bdocker[ -]compose\\b(\\s.+)?\\sup\\b)(\\s.+)?")
# https://docs.docker.com/reference/cli/docker/compose/up/
docker_compose_up_args = "--detach --wait --quiet-pull"

spark_sql_pattern = re.compile("(.*)bin/spark-sql(.*)")

curl_pattern = re.compile("(.*)\\bcurl\\b(.*)")


def tweak_docker_compose_up(code: List[str]) -> List[str]:
    """
    Tweaks a `docker compose up` in a code block to have the necessary options to
    start the containers in the background.
    :param code: Input code block lines.
    :return: Tweaked code block lines.
    """
    out: List[str] = []
    for line in code:
        match = docker_compose_up_pattern.match(line)
        if match:
            out.append(f"{match.group(1)} {docker_compose_up_args} {match.group(3) or ''}")
        else:
            out.append(line)
    return out


def tweak_spark_sql(code: List[str]) -> List[str]:
    """
    Tweaks a `spark-sql` command in a code block to use the local Spark installation
    referenced via the environment variable `SPARK_SQL_BIN` during execution of the
    generated script.
    :param code: Input code block lines.
    :return: Tweaked code block lines.
    """
    out: List[str] = []
    for line in code:
        match = spark_sql_pattern.match(line)
        if match:
            out.append(f"{match.group(1)}{'${SPARK_SQL_BIN}'}{match.group(2)}")
        else:
            out.append(line)
    return out


def tweak_curl(code: List[str]) -> List[str]:
    out: List[str] = []
    for line in code:
        match = curl_pattern.match(line)
        if match:
            out.append(f"{match.group(1)}curl --fail-with-body{match.group(2)}")
        else:
            out.append(line)
    return out


class CodeBlockBase:
    code_type: Optional[str]
    start_marker: str
    indent: int
    code: List[str]
    line_no: int
    skip_all: bool

    def __init__(
        self, line_no: int, start_marker: str, indent: int, code_type: Optional[str] = None, skip_all: bool = False
    ):
        self.line_no = line_no
        self.start_marker = start_marker
        self.indent = indent
        self.code = []
        self.nested_code_blocks = []
        self.code_type = code_type
        self.skip_all = skip_all

    def code_append(self, line: str) -> None:
        self.code.append(line[self.indent :].rstrip())

    def __str__(self) -> str:
        return (
            f"CodeBlock(line_no={self.line_no}, code_type={self.code_type!r}, "
            f"skip_all={self.skip_all}, indent={self.indent}, lines={len(self.code)})"
        )


class CodeBlock(CodeBlockBase):
    nested_code_blocks: List[CodeBlockBase]

    def __init__(
        self, line_no: int, start_marker: str, indent: int, code_type: Optional[str] = None, skip_all: bool = False
    ):
        super().__init__(line_no, start_marker, indent, code_type, skip_all)
        self.nested_code_blocks = []


class CodeBlocksGlobal:
    skipped: List[CodeBlockBase]

    def __init__(self):
        self.skipped = []

    def is_skipped(self, code_block: CodeBlockBase) -> bool:
        return any(cb.code_type == code_block.code_type and cb.code == code_block.code for cb in self.skipped)


# noinspection SpellCheckingInspection
def emit_code_block(f: TextIOBase, type_name: str, code: List[str], line_no: int, skipped: bool = False) -> None:
    """
    Emits a code block to the given file.
    :param f: The target output.
    :param type_name: Name of the code block type, like "shell" or "Spark SQL shell".
    :param code: Code block lines.
    :param line_no: Starting line number of the code block in the Markdown file.
    """
    f.write(
        f'echo "::group::Dump of {"skipped/repeated " if skipped else ""}{type_name} code block at line {line_no}"\n'
    )
    # Write the code block to be `echo`d so it is shown in the CI job log.
    f.write("cat <<CODE_SNIPPET_DUMP\n")
    for line in code:
        line = line.replace("\\", "\\\\")
        line = line.replace("$", "\\$")
        f.write(line + "\n")
    f.write("\nCODE_SNIPPET_DUMP\n")
    f.write('echo "::endgroup::"\n')
    if not skipped:
        # Write the code block to be executed.
        f.write(f'echo "::group::Execution of {type_name} code block at line {line_no}"\n')
        f.writelines("\n".join(code))
        f.write('\necho "::endgroup::"\n')
