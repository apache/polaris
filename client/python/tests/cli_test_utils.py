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

import io
import unittest
from typing import Any, Callable
from unittest.mock import patch, MagicMock

from apache_polaris.cli.command import Command
from apache_polaris.cli.options.parser import Parser
from apache_polaris.sdk.management import PolarisDefaultApi

INVALID_ARGS = 2


class CLITestBase(unittest.TestCase):
    def build_mock_client(self) -> MagicMock:
        client = MagicMock(spec=PolarisDefaultApi)
        client.api_client = MagicMock()
        client.api_client.configuration = MagicMock()
        client.api_client.configuration.host = "http://localhost:8080/api/management"
        client.api_client.configuration.proxy = None
        client.api_client.configuration.proxy_headers = None
        client.api_client.configuration.username = None
        client.api_client.configuration.password = None
        client.api_client.configuration.access_token = None
        client.api_client.default_headers = {}
        return client

    def mock_execute(self, mock_client: MagicMock, input_args: list[str]) -> Any:
        options = Parser.parse(input_args)
        command = Command.from_options(options)
        return command.execute(mock_client)

    def check_exception(self, func: Callable[[], Any], exception_str: str) -> None:
        with self.assertRaises(Exception) as cm:
            func()
        self.assertIn(exception_str, str(cm.exception))

    def check_usage_output(
        self, func: Callable[[], Any], needle: str = "usage:"
    ) -> None:
        with (
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
            patch("sys.stderr", new_callable=io.StringIO),
        ):
            with self.assertRaises(SystemExit) as cm:
                func()
            self.assertEqual(cm.exception.code, 0)
            output = mock_stdout.getvalue()
            self.assertIn(needle, output)
