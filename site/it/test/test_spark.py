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

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from site_checks.spark import SPARK_TGZ_FALLBACK_URL, SPARK_TGZ_URL, ensure_spark


class FakeStdout:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeProc:
    def __init__(self, rc: int, stdout: FakeStdout | None = None) -> None:
        self._rc = rc
        self.stdout = stdout
        self.wait_calls = 0

    def wait(self) -> int:
        self.wait_calls += 1
        return self._rc

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_ensure_spark_skips_download_when_present() -> None:
    spark_sql_bin = Path("/fake/spark/bin/spark-sql")

    with (
        patch("site_checks.spark.Path.exists", return_value=True),
        patch("site_checks.spark.subprocess.Popen") as popen_mock,
    ):
        ensure_spark(spark_sql_bin, Path("/fake/spark"), "http://example")

    popen_mock.assert_not_called()


def test_ensure_spark_downloads_and_extracts() -> None:
    spark_sql_bin = Path("/fake/spark/bin/spark-sql")
    curl_stdout = FakeStdout()
    curl_proc = FakeProc(0, stdout=curl_stdout)
    tar_proc = FakeProc(0)

    with (
        patch("site_checks.spark.Path.exists", return_value=False),
        patch("site_checks.spark.subprocess.Popen", side_effect=[curl_proc, tar_proc]) as popen_mock,
    ):
        ensure_spark(spark_sql_bin, Path("/fake/spark"), "http://example")

    popen_mock.assert_any_call(
        ["curl", "--location", "--fail", "--retry", "3", "http://example"], stdout=subprocess.PIPE
    )
    popen_mock.assert_any_call(
        [
            "tar",
            "--extract",
            "--gunzip",
            "--directory",
            "/fake/spark",
            "--strip-components=1",
        ],
        stdin=curl_proc.stdout,
    )
    assert curl_stdout.closed is True
    assert curl_proc.wait_calls == 1
    assert tar_proc.wait_calls == 1


def test_ensure_spark_raises_when_download_fails() -> None:
    spark_sql_bin = Path("/fake/spark/bin/spark-sql")
    curl_stdout = FakeStdout()
    curl_proc = FakeProc(0, stdout=curl_stdout)
    tar_proc = FakeProc(2)

    with (
        patch("site_checks.spark.Path.exists", return_value=False),
        patch("site_checks.spark.subprocess.Popen", side_effect=[curl_proc, tar_proc]),
    ):
        with pytest.raises(subprocess.CalledProcessError) as excinfo:
            ensure_spark(spark_sql_bin, Path("/fake/spark"), "http://example")

    assert excinfo.value.returncode == 2


def test_ensure_spark_falls_back_when_default_url_returns_invalid_tarball() -> None:
    spark_sql_bin = Path("/fake/spark/bin/spark-sql")
    procs = [FakeProc(0, stdout=FakeStdout()), FakeProc(2), FakeProc(0, stdout=FakeStdout()), FakeProc(0)]

    with (
        patch("site_checks.spark.Path.exists", return_value=False),
        patch("site_checks.spark.subprocess.Popen", side_effect=procs) as popen_mock,
    ):
        ensure_spark(spark_sql_bin, Path("/fake/spark"), SPARK_TGZ_URL)

    curl_urls = [c.args[0][-1] for c in popen_mock.call_args_list if c.args[0][0] == "curl"]
    assert curl_urls == [SPARK_TGZ_URL, SPARK_TGZ_FALLBACK_URL]


def test_ensure_spark_does_not_fall_back_for_custom_url() -> None:
    spark_sql_bin = Path("/fake/spark/bin/spark-sql")
    procs = [FakeProc(0, stdout=FakeStdout()), FakeProc(2)]

    with (
        patch("site_checks.spark.Path.exists", return_value=False),
        patch("site_checks.spark.subprocess.Popen", side_effect=procs),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            ensure_spark(spark_sql_bin, Path("/fake/spark"), "http://example")
