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
from unittest.mock import patch, MagicMock

from cli_test_utils import CLITestBase
from apache_polaris.cli.command.repl import ReplCommand, PolarisRepl
from apache_polaris.cli.constants import Commands
from apache_polaris.cli.exceptions import CliError
from apache_polaris.sdk.management.exceptions import ApiException


class TestReplCommand(CLITestBase):
    @patch("apache_polaris.cli.options.parser.Parser.parse")
    @patch("apache_polaris.cli.command.Command.from_options")
    def test_repl_executes_command(
        self, mock_from_options: MagicMock, mock_parse: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_command = MagicMock()
        mock_from_options.return_value = mock_command

        repl = PolarisRepl(mock_client)
        self.assertEqual(repl.prompt, "polaris@localhost:8080> ")

        repl.default("catalogs list")

        mock_parse.assert_called_with(["catalogs", "list"])
        mock_from_options.assert_called()
        mock_command.execute.assert_called_with(mock_client)

    @patch(
        "apache_polaris.cli.options.parser.Parser.parse",
        side_effect=RuntimeError("boom"),
    )
    def test_repl_handles_unexpected_exception(self, _mock_parse: MagicMock) -> None:
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
            repl.default("bad command")
        output = mock_stderr.getvalue()
        self.assertIn("An unexpected error occurred", output)
        self.assertIn("boom", output)

    @patch(
        "apache_polaris.cli.options.parser.Parser.parse",
        side_effect=CliError("bad usage"),
    )
    def test_repl_handles_cli_error(self, _mock_parse: MagicMock) -> None:
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
            repl.default("bad usage")
        output = mock_stderr.getvalue()
        self.assertIn("bad usage", output)
        self.assertNotIn("unexpected", output)

    @patch(
        "apache_polaris.cli.options.parser.Parser.parse",
        side_effect=NotImplementedError("nope"),
    )
    def test_repl_handles_not_implemented(self, _mock_parse: MagicMock) -> None:
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
            repl.default("something")
        output = mock_stderr.getvalue()
        self.assertIn("Internal error", output)
        self.assertIn("nope", output)

    @patch("apache_polaris.cli.polaris_cli.PolarisCli.print_api_exception")
    @patch("apache_polaris.cli.options.parser.Parser.parse")
    def test_repl_handles_api_exception(
        self, mock_parse: MagicMock, mock_print_api_exc: MagicMock
    ) -> None:
        api_exc = ApiException(status=500, reason="server error")
        mock_parse.side_effect = api_exc
        repl = PolarisRepl(self.build_mock_client())
        repl.default("catalogs list")
        mock_print_api_exc.assert_called_once_with(api_exc)

    @patch("apache_polaris.cli.options.parser.Parser.parse", side_effect=SystemExit(2))
    def test_repl_swallows_system_exit(self, _mock_parse: MagicMock) -> None:
        repl = PolarisRepl(self.build_mock_client())
        repl.default("--help")

    @patch("apache_polaris.cli.command.Command.from_options")
    @patch("apache_polaris.cli.options.parser.Parser.parse")
    def test_repl_in_repl_is_guarded(
        self, mock_parse: MagicMock, mock_from_options: MagicMock
    ) -> None:
        fake_options = MagicMock()
        fake_options.command = Commands.REPL
        mock_parse.return_value = fake_options
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
            repl.default("repl")
        mock_from_options.assert_not_called()
        self.assertIn("Already in REPL session", mock_stderr.getvalue())

    @patch(
        "apache_polaris.cli.options.parser.Parser.parse",
        side_effect=KeyboardInterrupt(),
    )
    def test_repl_handles_keyboard_interrupt_per_command(
        self, _mock_parse: MagicMock
    ) -> None:
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stderr", new_callable=io.StringIO) as mock_stderr:
            repl.default("catalogs list")
        self.assertIn("Session interrupted", mock_stderr.getvalue())

    def test_repl_empty_line_is_no_op(self) -> None:
        repl = PolarisRepl(self.build_mock_client())
        self.assertIsNone(repl.default(""))
        self.assertIsNone(repl.default("   "))

    @patch("apache_polaris.cli.command.repl.PolarisRepl")
    def test_repl_command_execute(self, mock_repl_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_repl_instance = mock_repl_class.return_value
        # Test with catalog
        ReplCommand(profile="test-profile", catalog="test-catalog").execute(mock_client)
        mock_repl_class.assert_called_with(
            mock_client, profile="test-profile", catalog="test-catalog"
        )
        # Test without catalog
        ReplCommand(profile="test-profile").execute(mock_client)
        mock_repl_class.assert_called_with(
            mock_client, profile="test-profile", catalog=None
        )
        self.assertEqual(mock_repl_instance.cmdloop.call_count, 2)

    def test_repl_prompt_resolution(self) -> None:
        mock_client = self.build_mock_client()
        # Profile takes precedence; otherwise show the api client's host:port.
        repl_with_profile = PolarisRepl(mock_client, profile="prod")
        self.assertEqual(repl_with_profile.prompt, "polaris@prod> ")
        repl_with_catalog = PolarisRepl(mock_client, catalog="test-catalog")
        self.assertEqual(
            repl_with_catalog.prompt, "polaris@localhost:8080/test-catalog> "
        )
        mock_client.api_client.configuration.host = (
            "http://resolved-host:9000/api/management/v1"
        )
        repl_default = PolarisRepl(mock_client)
        self.assertEqual(repl_default.prompt, "polaris@resolved-host:9000> ")

    @patch("apache_polaris.cli.options.parser.Parser.parse")
    @patch("apache_polaris.cli.command.Command.from_options")
    def test_repl_catalog_injestion(
        self, mock_from_options: MagicMock, mock_parse: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        repl = PolarisRepl(mock_client, catalog="test-catalog")
        fake_options = MagicMock()
        fake_options.catalog = None
        mock_parse.return_value = fake_options
        repl.default("namespaces list")
        self.assertEqual(fake_options.catalog, "test-catalog")
        mock_from_options.assert_called_with(fake_options)

    def test_emptyline_returns_false(self) -> None:
        repl = PolarisRepl(self.build_mock_client())
        self.assertFalse(repl.emptyline())

    def test_help_lists_polaris_commands(self) -> None:
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            repl.do_help("")
        out = mock_stdout.getvalue()
        self.assertIn("catalogs", out)
        self.assertIn("principals", out)
        self.assertIn("repl", out)
        self.assertIn("exit", out)

    def test_help_for_command_delegates_to_argparse(self) -> None:
        repl = PolarisRepl(self.build_mock_client())
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            repl.do_help("principals")
        self.assertIn("usage: polaris principals", mock_stdout.getvalue())
