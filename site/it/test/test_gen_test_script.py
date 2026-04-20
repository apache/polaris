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

from pathlib import Path
from textwrap import dedent

from site_checks.code_block import CodeBlocksGlobal
from site_checks.gen_test_script import generate_markdown_test_script


def test_generate_markdown_test_script_tweaks_shell(tmp_path: Path) -> None:
    markdown = tmp_path / "guide.md"
    output = tmp_path / "guide.sh"

    markdown.write_text(
        dedent(
            """
        # Guide
        ```shell
        docker compose up
        bin/spark-sql -e 'select 1'
        ```
        """
        ),
        encoding="utf-8",
    )

    generate_markdown_test_script(markdown, output, CodeBlocksGlobal())

    script = output.read_text(encoding="utf-8")
    assert "Code block type: shell" in script
    assert "Line number in markdown: 3" in script
    assert "docker compose up --detach --wait " in script
    assert "${SPARK_SQL_BIN} -e 'select 1'" in script
    assert output.stat().st_mode & 0o111


def test_generate_markdown_test_script_merges_sqlshell(tmp_path: Path) -> None:
    markdown = tmp_path / "guide.md"
    output = tmp_path / "guide.sh"

    markdown.write_text(
        dedent(
            """
        ```shell
        bin/spark-sql -e 'select 1'
        ```
        ```sql
        select 1;
        ```
        """
        ),
        encoding="utf-8",
    )

    generate_markdown_test_script(markdown, output, CodeBlocksGlobal())

    script = output.read_text(encoding="utf-8")
    assert "Code block type: sqlshell" in script
    assert "Write SQL file for" in script
    assert "-- SQL code block starting at line 5" in script
    assert "select 1;" in script
    assert "${SPARK_SQL_BIN} -e 'select 1' \\\n -f \"${BUILD_TESTS_DIR}/.current.sql\"" in script


def test_generate_markdown_test_script_handles_empty_block(tmp_path: Path) -> None:
    markdown = tmp_path / "guide.md"
    output = tmp_path / "guide.sh"

    markdown.write_text(
        dedent(
            """
        ```shell
        ```
        """
        ),
        encoding="utf-8",
    )

    generate_markdown_test_script(markdown, output, CodeBlocksGlobal())

    script = output.read_text(encoding="utf-8")
    assert "# Code block is empty" in script
