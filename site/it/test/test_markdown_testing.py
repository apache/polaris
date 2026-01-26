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
from unittest.mock import patch

import pytest

from site_checks import markdown_testing
from site_checks.code_block import CodeBlocksGlobal


class FakeTee:
    run_return = 0
    last_instance = None

    # noinspection PyUnusedLocal
    def __init__(self, *files) -> None:
        FakeTee.last_instance = self
        self.printf_calls = []
        self.run_calls = []

    def printf(self, *args: object) -> None:
        self.printf_calls.append(" ".join(str(arg) for arg in args))

    def run(self, cmd, **kwargs) -> int:
        self.run_calls.append((list(cmd), kwargs))
        return self.run_return


def _fake_generate_markdown_test_script(_md_file, output_file, code_blocks_global) -> None:
    Path(output_file).write_text("#!/usr/bin/env bash\n", encoding="utf-8")


def test_run_test_success_writes_summary_and_env(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    workspace_dir = tmp_path / "workspace"
    build_tests_dir = tmp_path / "build-tests"
    md_file = site_dir / "content" / "guides" / "intro" / "guide.md"
    test_summary_file = build_tests_dir / "test-summary.txt"
    github_summary = tmp_path / "github-summary.txt"

    md_file.parent.mkdir(parents=True)
    build_tests_dir.mkdir(parents=True)
    md_file.write_text("# Guide\n", encoding="utf-8")

    env = {"GITHUB_STEP_SUMMARY": str(github_summary)}

    FakeTee.run_return = 0
    with (
        patch("site_checks.markdown_testing.Tee", FakeTee),
        patch("site_checks.markdown_testing.generate_markdown_test_script", _fake_generate_markdown_test_script),
        patch("site_checks.markdown_testing.cleanup_docker") as cleanup_mock,
        patch("site_checks.markdown_testing.docker_compose_info") as docker_info_mock,
    ):
        result = markdown_testing.run_test(
            md_file=md_file,
            site_dir=site_dir,
            workspace_dir=workspace_dir,
            build_tests_dir=build_tests_dir,
            test_summary_file=test_summary_file,
            env=env,
            code_blocks_global=CodeBlocksGlobal(),
        )

    assert result is True
    assert f"✅ Test passed for {md_file}" in test_summary_file.read_text(encoding="utf-8")
    assert f"✅ Test passed for {md_file}" in github_summary.read_text(encoding="utf-8")
    cleanup_mock.assert_called()
    docker_info_mock.assert_called_once()
    assert FakeTee.last_instance is not None
    run_env = FakeTee.last_instance.run_calls[0][1]["env"]
    assert run_env["SITE_TEST_GUIDE_DIR"] == str(site_dir / "content" / "guides" / "intro")


def test_run_test_failure_marks_summary(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    workspace_dir = tmp_path / "workspace"
    build_tests_dir = tmp_path / "build-tests"
    md_file = site_dir / "content" / "guides" / "intro" / "guide.md"
    test_summary_file = build_tests_dir / "test-summary.txt"

    md_file.parent.mkdir(parents=True)
    build_tests_dir.mkdir(parents=True)
    md_file.write_text("# Guide\n", encoding="utf-8")

    env = {}

    FakeTee.run_return = 1
    with (
        patch("site_checks.markdown_testing.Tee", FakeTee),
        patch("site_checks.markdown_testing.generate_markdown_test_script", _fake_generate_markdown_test_script),
        patch("site_checks.markdown_testing.cleanup_docker"),
        patch("site_checks.markdown_testing.docker_compose_info"),
    ):
        result = markdown_testing.run_test(
            md_file=md_file,
            site_dir=site_dir,
            workspace_dir=workspace_dir,
            build_tests_dir=build_tests_dir,
            test_summary_file=test_summary_file,
            env=env,
            code_blocks_global=CodeBlocksGlobal(),
        )

    assert result is False
    assert "Test failed for" in test_summary_file.read_text(encoding="utf-8")
    assert FakeTee.last_instance is not None
    assert "::endgroup::" in FakeTee.last_instance.printf_calls


def test_run_test_rejects_non_markdown(tmp_path: Path) -> None:
    site_dir = tmp_path / "site"
    md_file = site_dir / "content" / "guides" / "intro" / "guide.txt"

    md_file.parent.mkdir(parents=True)
    md_file.write_text("not markdown", encoding="utf-8")

    with pytest.raises(Exception, match="not a markdown file"):
        markdown_testing.run_test(
            md_file=md_file,
            site_dir=site_dir,
            workspace_dir=tmp_path,
            build_tests_dir=tmp_path,
            test_summary_file=tmp_path / "summary.txt",
            env={},
            code_blocks_global=CodeBlocksGlobal(),
        )


def test_markdown_testing_help_calls_usage() -> None:
    with patch("site_checks.markdown_testing.usage") as usage_mock:
        assert markdown_testing.markdown_testing(["-h"]) is True

    usage_mock.assert_called_once()


def test_markdown_testing_unknown_option_reports_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert markdown_testing.markdown_testing(["--nope"]) is False

    captured = capsys.readouterr()
    assert "Unknown option" in captured.err
