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
from io import StringIO
from unittest.mock import patch

import pytest

from site_checks.tee import Tee


class Recorder:
    def __init__(self) -> None:
        self.writes = []
        self.flushes = 0

    def write(self, data: str) -> None:
        self.writes.append(data)

    def flush(self) -> None:
        self.flushes += 1


class FakeStdout:
    def __init__(self, lines: list[str]) -> None:
        self._iter = iter(lines)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self) -> str:
        return next(self._iter)

    def close(self) -> None:
        self.closed = True


class FakeProc:
    def __init__(self, lines: list[str], rc: int) -> None:
        self.stdout = FakeStdout(lines)
        self._rc = rc

    def wait(self) -> int:
        return self._rc


def test_tee_write_and_flush() -> None:
    first = Recorder()
    second = Recorder()
    tee = Tee(first, second)

    tee.write("hello")

    assert first.writes == ["hello"]
    assert second.writes == ["hello"]
    assert first.flushes == 1
    assert second.flushes == 1

    tee.flush()

    assert first.flushes == 2
    assert second.flushes == 2


def test_tee_run_writes_output_and_returns_rc() -> None:
    recorder = Recorder()
    tee = Tee(recorder)
    proc = FakeProc(["first\n", "second\n"], rc=0)

    with patch("site_checks.tee.subprocess.Popen", return_value=proc) as popen_mock:
        rc = tee.run(["echo", "test"], cwd=None, env={"A": "B"})

    popen_mock.assert_called_once_with(
        ["echo", "test"],
        cwd=None,
        env={"A": "B"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    assert recorder.writes == ["first\n", "second\n"]
    assert proc.stdout.closed is True
    assert rc == 0


def test_tee_run_raises_on_check_failure() -> None:
    tee = Tee(Recorder())
    proc = FakeProc([], rc=5)

    with patch("site_checks.tee.subprocess.Popen", return_value=proc):
        with pytest.raises(subprocess.CalledProcessError):
            tee.run(["nope"], check=True)


def test_tee_printf_writes_to_files() -> None:
    output = StringIO()
    tee = Tee(output)

    tee.printf("hello", "world")

    assert output.getvalue() == "hello world\n"
