#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

# Transforms a Markdown file into a bash script that can be used to test a guide.
# An `sql` code block preceded by a `shell` code block is transformed into a `sqlshell`
# code block if the first line of the `shell` code block contains `bin/spark-sql`.

# "Assertions" and other executions for the guide-tests can be added to the
# Markdown file by adding a `shell` code block inside an HTML comment.
# The code block will be executed but not appear in the guide.
#
# <!--
# ```shell
# # add shell script code to test things
# ```
# -->

# Markdown code blocks are opened with _at least_ 3 backtick,
# optionally preceded by whitespaces,
# optionally followed by a language identifier.
# Must check that the close marker is the same as the open marker,
# excluding the language identifier.
#
# "Normal" code block example:
# ```shell
# some-shell-code
# ```
#
# Indented code block example:
#     ```shell
#     some-shell-code
#     ```
#
# A code block that includes 3 backticks can be written as:
# ````shell
# some-shell-code - the backticks are part of the code block!
# ```
# ````
#

import re
from os import chmod
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

from .code_block import (
    CodeBlock,
    CodeBlocksGlobal,
    emit_code_block,
    tweak_curl,
    tweak_docker_compose_up,
    tweak_spark_sql,
)

open_pattern = re.compile("((\\s*)`{3,})([a-z0-9-_:]+)?\\s*(\\bskip_all\\b)?\\s*")
close_pattern = re.compile("(\\s*`{3,})")


def generate_markdown_test_script(markdown_file: Path, output_file: Path, code_blocks_global: CodeBlocksGlobal) -> None:
    """
    Parses a Markdown file and generates a bash script that can be used to test a guide.

    Constraints:
    1. `docker compose` invocations must be on a single line in a `shell` code block.
    2. Spark SQL shell invocations must be the only statement `shell` code block in a guide.

    :param markdown_file: The Markdown file to process.
    :param output_file: The output bash script file to generate.
    :param code_blocks_global: Global state for code blocks, including skipped blocks.
    """
    code_blocks: List[CodeBlock] = []
    with open(markdown_file) as f:
        code_block: Optional[CodeBlock] = None
        line_no = 1
        for line in f.readlines():
            if not code_block:
                open_match = re.match(open_pattern, line)
                if open_match is not None:
                    code_block = CodeBlock(
                        line_no=line_no,
                        start_marker=open_match.group(1),
                        indent=len(open_match.group(2)),
                        code_type=open_match.group(3),
                        skip_all=open_match.group(4) is not None,
                    )
            else:
                close_match = re.match(close_pattern, line)
                if close_match and close_match.group(1) == code_block.start_marker:
                    num_code_blocks = len(code_blocks)
                    if num_code_blocks > 0:
                        prev_code_block = code_blocks[num_code_blocks - 1]
                        if code_block.code_type == "sql" and (
                            (
                                prev_code_block.code_type == "shell"
                                and len(prev_code_block.code) > 0
                                and "bin/spark-sql" in prev_code_block.code[0]
                            )
                            or prev_code_block.code_type == "sqlshell"
                        ):
                            prev_code_block.code_type = "sqlshell"
                            prev_code_block.nested_code_blocks.append(code_block)
                        else:
                            code_blocks.append(code_block)
                    else:
                        code_blocks.append(code_block)
                    code_block = None
                else:
                    line = line.rstrip()
                    if len(code_block.code) > 0 or len(line) > 0:
                        code_block.code_append(line)
            line_no += 1

    with open(output_file, mode="wt") as f:
        f.write("#!/usr/bin/env bash\n")
        f.write("set -e\n")
        f.write("set -o pipefail\n")
        f.write(
            dedent(f"""
             #
             # Code blocks from markdown source: {markdown_file}
             ##################################################################
             #
             """)
        )
        for code_block in code_blocks:
            f.write(
                dedent(f"""
                 #
                 # Code block type: {code_block.code_type}
                 # Line number in markdown: {code_block.line_no}
                 ##################################################################
                 #
                 """)
            )
            if len(code_block.code) == 0:
                f.write("# Code block is empty\n")
                continue

            if not code_block.code_type:
                continue

            if code_block.skip_all:
                code_blocks_global.skipped.append(code_block)
            elif code_block.code_type == "shell":
                code = tweak_docker_compose_up(code_block.code)
                code = tweak_curl(code)
                code = tweak_spark_sql(code)
                emit_code_block(
                    f=f,
                    type_name="shell",
                    code=code,
                    line_no=code_block.line_no,
                    skipped=code_blocks_global.is_skipped(code_block),
                )
            elif code_block.code_type == "sqlshell":
                sql_code: List[str] = ['cat > "${BUILD_TESTS_DIR}/.current.sql" <<SQL_SNIPPET_EOF']
                for nested_code_block in code_block.nested_code_blocks:
                    sql_code.append("-" * 67)
                    sql_code.append(f"-- SQL code block starting at line {nested_code_block.line_no}")
                    sql_code += nested_code_block.code
                sql_code.append("SQL_SNIPPET_EOF")
                emit_code_block(f=f, type_name="Write SQL file for", code=sql_code, line_no=code_block.line_no)

                code = tweak_spark_sql(code_block.code)
                combined_code: List[str] = []
                combined_code += code[:-1]
                last_code_line = code[-1]
                combined_code.append(last_code_line + " \\")
                combined_code.append(' -f "${BUILD_TESTS_DIR}/.current.sql"')
                combined_code.append('rm -f "${BUILD_TESTS_DIR}/.current.sql"')
                emit_code_block(f=f, type_name="Spark SQL shell", code=combined_code, line_no=code_block.line_no)
            else:
                f.writelines("\n".join([f"# {line}" for line in code_block.code]))
                f.write("\n")
    chmod(output_file, 0o755)
