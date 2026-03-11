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

from types import SimpleNamespace
from unittest.mock import patch

from site_checks.docker import cleanup_docker, docker_compose_info
from site_checks.tee import Tee


class TeeRecorder(Tee):
    def __init__(self, *files) -> None:
        super().__init__(*files)
        self.printf_calls = []
        self.run_calls = []

    def printf(self, *args: object) -> None:
        self.printf_calls.append(" ".join(str(arg) for arg in args))

    def run(self, cmd, **kwargs) -> int:
        self.run_calls.append((list(cmd), kwargs))
        return 0


class TeeRaiser(TeeRecorder):
    def run(self, cmd, **kwargs) -> int:
        self.run_calls.append((list(cmd), kwargs))
        raise FileNotFoundError


def test_cleanup_docker_removes_containers_and_prunes() -> None:
    tee = TeeRecorder()
    result = SimpleNamespace(stdout="abc\n\nxyz\n")

    with patch("site_checks.docker.subprocess.run", return_value=result) as run_mock:
        cleanup_docker(tee)

    run_mock.assert_called_once_with(
        ["docker", "ps", "-a", "-q"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert tee.run_calls == [
        (["docker", "container", "rm", "-f", "abc", "xyz"], {}),
        (["docker", "network", "prune", "-f"], {}),
        (["docker", "volume", "prune", "-f"], {}),
    ]


def test_docker_compose_info_logs_entries() -> None:
    tee = TeeRecorder()
    result = SimpleNamespace(stdout='[{"ConfigFiles":"compose-a.yml"},{"ConfigFiles":"compose-b.yml"}]')

    with patch("site_checks.docker.subprocess.run", return_value=result):
        docker_compose_info(tee, "readme", "docs")

    assert tee.run_calls == [
        (
            [
                "docker",
                "compose",
                "-f",
                "compose-a.yml",
                "ps",
                "--all",
                "--no-trunc",
                "--orphans",
            ],
            {},
        ),
        (
            [
                "docker",
                "compose",
                "-f",
                "compose-a.yml",
                "logs",
                "--timestamps",
            ],
            {},
        ),
        (
            [
                "docker",
                "compose",
                "-f",
                "compose-b.yml",
                "ps",
                "--all",
                "--no-trunc",
                "--orphans",
            ],
            {},
        ),
        (
            [
                "docker",
                "compose",
                "-f",
                "compose-b.yml",
                "logs",
                "--timestamps",
            ],
            {},
        ),
    ]


def test_docker_compose_info_ignores_invalid_json() -> None:
    tee = TeeRecorder()
    result = SimpleNamespace(stdout="not-json")

    with patch("site_checks.docker.subprocess.run", return_value=result):
        docker_compose_info(tee, "readme", "docs")

    assert tee.run_calls == []
