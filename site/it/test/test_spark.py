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

from site_checks.spark import DOWNLOAD_ATTEMPTS, ensure_spark


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

    popen_mock.assert_any_call(["curl", "--location", "--fail", "http://example"], stdout=subprocess.PIPE)
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
    procs = [p for _ in range(DOWNLOAD_ATTEMPTS) for p in (FakeProc(0, stdout=FakeStdout()), FakeProc(2))]

    with (
        patch("site_checks.spark.Path.exists", return_value=False),
        patch("site_checks.spark.subprocess.Popen", side_effect=procs),
        patch("site_checks.spark.time.sleep") as sleep_mock,
    ):
        with pytest.raises(subprocess.CalledProcessError) as excinfo:
            ensure_spark(spark_sql_bin, Path("/fake/spark"), "http://example")

    assert excinfo.value.returncode == 2
    assert sleep_mock.call_count == DOWNLOAD_ATTEMPTS - 1


def test_ensure_spark_retries_with_exponential_backoff_until_success() -> None:
    spark_sql_bin = Path("/fake/spark/bin/spark-sql")
    procs = [
        FakeProc(0, stdout=FakeStdout()),
        FakeProc(2),
        FakeProc(0, stdout=FakeStdout()),
        FakeProc(2),
        FakeProc(0, stdout=FakeStdout()),
        FakeProc(0),
    ]

    with (
        patch("site_checks.spark.Path.exists", return_value=False),
        patch("site_checks.spark.subprocess.Popen", side_effect=procs) as popen_mock,
        patch("site_checks.spark.time.sleep") as sleep_mock,
    ):
        ensure_spark(spark_sql_bin, Path("/fake/spark"), "http://example")

    assert popen_mock.call_count == 6
    delays = [c.args[0] for c in sleep_mock.call_args_list]
    assert len(delays) == 2
    assert 1.0 <= delays[0] <= 2.0
    assert 2.0 <= delays[1] <= 4.0
