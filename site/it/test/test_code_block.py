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

from io import StringIO

from site_checks.code_block import (
    CodeBlock,
    CodeBlockBase,
    emit_code_block,
    tweak_curl,
    tweak_docker_compose_up,
    tweak_spark_sql,
)


def test_code_block_base_initializes_fields() -> None:
    block = CodeBlockBase(line_no=3, start_marker="```", indent=0, code_type="shell")

    assert block.line_no == 3
    assert block.start_marker == "```"
    assert block.code_type == "shell"
    assert block.code == []
    assert block.nested_code_blocks == []


def test_code_block_initializes_nested_blocks() -> None:
    block = CodeBlock(line_no=8, start_marker="```sql", indent=0, code_type=None)

    assert block.line_no == 8
    assert block.start_marker == "```sql"
    assert block.code_type is None
    assert block.code == []
    assert block.nested_code_blocks == []


def test_emit_code_block_writes_dump_and_execution_sections() -> None:
    output = StringIO()
    code = ["echo $HOME", r"path\to\file", "plain"]

    emit_code_block(output, "shell", code, line_no=12)

    expected = "".join(
        [
            'echo "::group::Dump of shell code block at line 12"\n',
            "cat <<CODE_SNIPPET_DUMP\n",
            "echo \\$HOME\n",
            "path\\\\to\\\\file\n",
            "plain\n",
            "\nCODE_SNIPPET_DUMP\n",
            'echo "::endgroup::"\n',
            'echo "::group::Execution of shell code block at line 12"\n',
            "echo $HOME\n",
            r"path\to\file",
            "\nplain\n",
            'echo "::endgroup::"\n',
        ]
    )

    assert output.getvalue() == expected


def test_tweak_docker_compose_up_adds_args() -> None:
    code = [
        "docker compose up",
        "docker compose up --build",
        "docker-compose -f docker-compose.yml up",
    ]

    assert tweak_docker_compose_up(code) == [
        "docker compose up --detach --wait --quiet-pull ",
        "docker compose up --detach --wait --quiet-pull  --build",
        "docker-compose -f docker-compose.yml up --detach --wait --quiet-pull ",
    ]


def test_tweak_docker_compose_up_leaves_non_matches() -> None:
    code = [
        "echo docker compose up",
        "docker ps",
    ]

    assert tweak_docker_compose_up(code) == code


def test_tweak_spark_sql_rewrites_binary() -> None:
    code = [
        "bin/spark-sql --foo",
        "  /opt/spark/bin/spark-sql -e 'select 1'",
    ]

    assert tweak_spark_sql(code) == [
        "${SPARK_SQL_BIN} --foo",
        "  /opt/spark/${SPARK_SQL_BIN} -e 'select 1'",
    ]


def test_tweak_spark_sql_leaves_non_matches() -> None:
    code = ["echo spark-sql", "bin/spark", "spark-sql"]

    assert tweak_spark_sql(code) == code


# noinspection HttpUrlsUsage
def test_tweak_curl_adds_fail_with_body() -> None:
    code = [
        "curl http://example.com",
        "  curl -X POST http://example.com",
        "/usr/bin/curl --head http://example.com",
    ]

    assert tweak_curl(code) == [
        "curl --fail-with-body http://example.com",
        "  curl --fail-with-body -X POST http://example.com",
        "/usr/bin/curl --fail-with-body --head http://example.com",
    ]


# noinspection HttpUrlsUsage
def test_tweak_curl_leaves_non_matches() -> None:
    code = [
        "echo curling",
        "scurl http://example.com",
    ]

    assert tweak_curl(code) == code
