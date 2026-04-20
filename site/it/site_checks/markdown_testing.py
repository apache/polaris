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

from __future__ import annotations

import getopt
import os
import sys
import time
from pathlib import Path
from textwrap import dedent
from typing import List

from .code_block import CodeBlocksGlobal
from .docker import cleanup_docker, docker_compose_info
from .gen_test_script import generate_markdown_test_script
from .spark import SPARK_TGZ_URL, ensure_spark
from .tee import Tee


def _format_duration(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    rem_seconds = int(round(seconds - (minutes * 60)))
    if rem_seconds == 60:
        minutes += 1
        rem_seconds = 0
    return f"{minutes}m {rem_seconds:02d}s"


def run_test(
    md_file: Path,
    site_dir: Path,
    workspace_dir: Path,
    build_tests_dir: Path,
    test_summary_file: Path,
    env: dict[str, str],
    code_blocks_global: CodeBlocksGlobal,
) -> bool:
    """
    Exercises testing of a single Markdown file.

    :param md_file: The Markdown file to test.
    :param site_dir: The `site/` directory of the workspace.
    :param workspace_dir: Root of the Polaris worktree directory.
    :param build_tests_dir: Directory to place test scripts and logs.
    :param test_summary_file: Summary file for the test results.
    :param env: Environment variables to pass to the test script.
    :return: `True` if the test ran successfully, `False` otherwise.
    """
    if not md_file.is_file():
        raise Exception(f"Error: {md_file} does not exist or is not a file")
    if md_file.suffix != ".md":
        raise Exception(f"Error: {md_file} is not a markdown file")

    try:
        md_rel_path = md_file.resolve().relative_to(site_dir.resolve())
    except ValueError:
        raise Exception(f"Error: {md_file} must be inside {site_dir}")

    md_rel_name = md_rel_path.as_posix()
    md_base_name = md_file.name
    md_dir_name = str(md_rel_path.parent)
    test_name = md_rel_name.replace("/", "_-_")
    test_script = build_tests_dir / f"{test_name}.sh"
    log_file = build_tests_dir / f"{test_name}.log"

    with log_file.open("w", encoding="utf-8") as log_handle:
        tee = Tee(sys.stdout, log_handle)
        tee.printf(">")
        tee.printf(f"> Testing {md_base_name} in {md_dir_name} ...")

        generate_markdown_test_script(md_file, test_script, code_blocks_global)

        cleanup_docker(tee)
        env = dict(env)
        env["SITE_TEST_GUIDE_DIR"] = str(site_dir / md_dir_name)
        test_start = time.monotonic()
        failed = tee.run([str(test_script)], cwd=workspace_dir, env=env) != 0
        test_duration = time.monotonic() - test_start
        # Unconditionally emit an `::endgroup::` in case the generated script failed to emit its own.
        tee.printf("::endgroup::")

        duration_str = _format_duration(test_duration)
        if failed:
            summary = f"❌ Test failed for {md_file} ({duration_str})"
        else:
            summary = f"✅ Test passed for {md_file} ({duration_str})"

        tee.printf(f"{summary}")
        with test_summary_file.open("a", encoding="utf-8") as summary_handle:
            summary_handle.write(f"{summary}\n")
        if env.get("GITHUB_STEP_SUMMARY"):
            with open(env["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary_out:
                summary_out.write(f"{summary}\n")
        tee.printf(">")

        docker_info_duration = docker_compose_info(tee, md_base_name, md_dir_name)
        cleanup_duration = cleanup_docker(tee)

        tee.printf(
            dedent(f"""
            Test steps durations for for {md_file} ({"❌ FAILED" if failed else "✅ passed"}):
                .. Test: {duration_str}
                .. Docker Compose Info: {_format_duration(docker_info_duration)}
                .. Docker Cleanup: {_format_duration(cleanup_duration)}
            """)
        )
        tee.printf("*" * 96)

    return not failed


def usage() -> None:
    sys.stderr.write(
        dedent(
            f"""
        Usage: markdown-testing.sh [options] [<markdown-file> ...]

        markdown-files MUST be inside the site/ directory.
        If no markdown-files are specified on the command line,
        all markdown-files in site/content/guides are tested.

        Options:
          -s <spark-tarball>
          --spark-tarball=<spark-tarball>
               URL to download the Spark tarball.
               Default: {SPARK_TGZ_URL}.
          -S
          --download-spark-tarball
               Always download the Spark tarball.
          -h
          --help  
               Show this help text.
        """
        )
    )


def markdown_testing(args: List[str]) -> bool:
    """
    Main entry point for Markdown code block testing.

    :param args: Command line arguments.
    :return: `False` indicates an error occurred, `True` otherwise.
    """
    try:
        opts, args = getopt.getopt(args, "hs:S", ["help", "spark-tarball=", "download-spark-tarball"])
    except getopt.GetoptError as exc:
        sys.stderr.write(f"Unknown option: -{exc.opt}\n")
        usage()
        return False

    spark_tarball_url = SPARK_TGZ_URL
    spark_download_requested = False

    for opt, val in opts:
        if opt in ("-h", "--help"):
            usage()
            return True
        elif opt in ("-s", "--spark-tarball"):
            spark_tarball_url = val
        elif opt in ("-S", "--download-spark-tarball"):
            spark_download_requested = True

    script_dir = Path(__file__).resolve().parent.parent
    site_dir = script_dir.parent

    build_dir = site_dir / "it" / "build"
    build_tests_dir = build_dir / "tests"
    spark_dir = build_dir / "spark"
    build_tests_dir.mkdir(parents=True, exist_ok=True)
    spark_dir.mkdir(parents=True, exist_ok=True)

    spark_sql_bin = spark_dir / "bin" / "spark-sql"
    ensure_spark(
        spark_sql_bin=spark_sql_bin,
        spark_dir=spark_dir,
        spark_tarball_url=spark_tarball_url,
        spark_download_requested=spark_download_requested,
    )

    workspace_dir = (site_dir / "..").resolve()

    env = dict(os.environ)
    env["SITE_TEST_WORKSPACE_DIR"] = str(workspace_dir)
    env["SPARK_SQL_BIN"] = str(spark_sql_bin)
    env["BUILD_TESTS_DIR"] = str(build_tests_dir)

    if args:
        raw_files = list(Path(arg) for arg in args)
    else:
        raw_files = (site_dir / "content" / "guides").glob("*/*.md")
    md_files = sorted(p.absolute().relative_to(site_dir) for p in raw_files)

    test_summary_file = build_tests_dir / "test-summary.txt"
    if test_summary_file.exists():
        test_summary_file.unlink()
    test_summary_file.touch()

    code_blocks_global = CodeBlocksGlobal()

    os.chdir(site_dir)
    all_successful = True
    for md_file in md_files:
        all_successful &= run_test(
            md_file=md_file,
            site_dir=site_dir,
            workspace_dir=workspace_dir,
            build_tests_dir=build_tests_dir,
            test_summary_file=test_summary_file,
            env=env,
            code_blocks_global=code_blocks_global,
        )

    print(">")

    print("************************************************************************************************")
    print("Test summary:")
    with test_summary_file.open(encoding="utf-8") as summary_handle:
        sys.stdout.write(summary_handle.read())

    return all_successful
