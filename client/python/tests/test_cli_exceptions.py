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
from unittest.mock import patch

from apache_polaris.cli.exceptions import (
    CliError,
    CLI_ERROR_EXIT_CODE,
    CLI_USAGE_EXIT_CODE,
)
from apache_polaris.cli.options.parser import Parser
from apache_polaris.cli.polaris_cli import PolarisCli


class TestCliExceptions(unittest.TestCase):
    def test_cli_error_default_exit_code(self) -> None:
        err = CliError("bad input")
        self.assertEqual(err.exit_code, CLI_USAGE_EXIT_CODE)
        self.assertEqual(str(err), "bad input")

    def test_cli_error_custom_exit_code(self) -> None:
        err = CliError("token failed", exit_code=CLI_ERROR_EXIT_CODE)
        self.assertEqual(err.exit_code, CLI_ERROR_EXIT_CODE)

    def test_parse_properties_duplicate_key(self) -> None:
        with self.assertRaises(CliError) as cm:
            Parser.parse_properties(["a=1", "a=2"])
        self.assertIn("Duplicate property key", str(cm.exception))

    def test_polaris_cli_exits_on_cli_error(self) -> None:
        stderr = io.StringIO()
        with (
            patch("apache_polaris.cli.polaris_cli.sys.stderr", stderr),
            patch(
                "apache_polaris.cli.polaris_cli.Parser.parse",
                side_effect=CliError("unit test failure"),
            ),
            patch(
                "apache_polaris.cli.polaris_cli.sys.exit",
                side_effect=SystemExit(CLI_USAGE_EXIT_CODE),
            ),
        ):
            with self.assertRaises(SystemExit) as cm:
                PolarisCli.execute(["catalogs", "list"])
        self.assertEqual(cm.exception.code, CLI_USAGE_EXIT_CODE)
        self.assertIn("unit test failure", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
