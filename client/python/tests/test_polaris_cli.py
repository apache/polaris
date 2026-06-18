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
from argparse import Namespace
from unittest.mock import MagicMock, patch

import urllib3

from apache_polaris.cli.constants import Commands
from apache_polaris.cli.polaris_cli import PolarisCli


class TestPolarisCliDebugLogging(unittest.TestCase):
    def tearDown(self) -> None:
        if hasattr(urllib3.PoolManager, "original_urlopen"):
            urllib3.PoolManager.urlopen = urllib3.PoolManager.original_urlopen
            del urllib3.PoolManager.original_urlopen

    def _debug_options(self, *, debug: bool) -> Namespace:
        return Namespace(debug=debug, command=Commands.CATALOGS)

    @patch("apache_polaris.cli.polaris_cli.Command.from_options")
    @patch("apache_polaris.cli.polaris_cli.PolarisDefaultApi")
    @patch("apache_polaris.cli.polaris_cli.ApiClientBuilder")
    @patch("apache_polaris.cli.polaris_cli.PolarisCli._enable_api_request_logging")
    @patch("apache_polaris.cli.polaris_cli.Parser.parse")
    def test_debug_logging_enabled_before_client_creation(
        self,
        mock_parse: MagicMock,
        mock_enable: MagicMock,
        mock_builder: MagicMock,
        _mock_default_api: MagicMock,
        mock_from_options: MagicMock,
    ) -> None:
        mock_parse.return_value = self._debug_options(debug=True)
        mock_builder_instance = MagicMock()
        mock_builder.return_value = mock_builder_instance
        mock_builder_instance.get_api_client.return_value = MagicMock()
        mock_from_options.return_value = MagicMock()

        call_order: list[str] = []
        mock_enable.side_effect = lambda: call_order.append("enable_logging")
        mock_builder_instance.get_api_client.side_effect = lambda: (
            call_order.append("get_api_client") or MagicMock()
        )

        PolarisCli.execute(
            ["--debug", "--access-token", "token", "catalogs", "list"]
        )

        mock_enable.assert_called_once()
        mock_builder_instance.get_api_client.assert_called_once()
        self.assertEqual(call_order, ["enable_logging", "get_api_client"])

    @patch("apache_polaris.cli.polaris_cli.Command.from_options")
    @patch("apache_polaris.cli.polaris_cli.PolarisDefaultApi")
    @patch("apache_polaris.cli.polaris_cli.ApiClientBuilder")
    @patch("apache_polaris.cli.polaris_cli.PolarisCli._enable_api_request_logging")
    @patch("apache_polaris.cli.polaris_cli.Parser.parse")
    def test_debug_logging_not_enabled_when_debug_disabled(
        self,
        mock_parse: MagicMock,
        mock_enable: MagicMock,
        mock_builder: MagicMock,
        _mock_default_api: MagicMock,
        mock_from_options: MagicMock,
    ) -> None:
        mock_parse.return_value = self._debug_options(debug=False)
        mock_builder_instance = MagicMock()
        mock_builder.return_value = mock_builder_instance
        mock_builder_instance.get_api_client.return_value = MagicMock()
        mock_from_options.return_value = MagicMock()

        PolarisCli.execute(["--access-token", "token", "catalogs", "list"])

        mock_enable.assert_not_called()
        mock_builder_instance.get_api_client.assert_called_once()

    @patch("apache_polaris.cli.polaris_cli.Command.from_options")
    @patch("apache_polaris.cli.polaris_cli.PolarisDefaultApi")
    @patch("apache_polaris.cli.polaris_cli.ApiClientBuilder")
    @patch("apache_polaris.cli.polaris_cli.Parser.parse")
    def test_client_creation_and_command_execution_still_work(
        self,
        mock_parse: MagicMock,
        mock_builder: MagicMock,
        mock_default_api: MagicMock,
        mock_from_options: MagicMock,
    ) -> None:
        mock_parse.return_value = self._debug_options(debug=False)
        api_client = MagicMock()
        mock_builder_instance = MagicMock()
        mock_builder.return_value = mock_builder_instance
        mock_builder_instance.get_api_client.return_value = api_client
        mock_command = MagicMock()
        mock_from_options.return_value = mock_command
        admin_api = MagicMock()
        mock_default_api.return_value = admin_api

        PolarisCli.execute(["--access-token", "token", "catalogs", "list"])

        mock_builder.assert_called_once()
        mock_builder_instance.get_api_client.assert_called_once()
        mock_default_api.assert_called_once_with(api_client)
        mock_command.execute.assert_called_once_with(admin_api)

    @patch("apache_polaris.cli.polaris_cli.sys.stderr", new_callable=io.StringIO)
    def test_enable_api_request_logging_logs_requests(
        self, mock_stderr: io.StringIO
    ) -> None:
        mock_response = MagicMock()
        with patch.object(
            urllib3.PoolManager,
            "original_urlopen",
            return_value=mock_response,
            create=True,
        ) as mock_original_urlopen:
            PolarisCli._enable_api_request_logging()
            pool = urllib3.PoolManager()
            pool.urlopen(
                "GET",
                "http://localhost:8080/api/management/v1/catalogs",
                headers={"Authorization": "Bearer secret-token"},
                body=b"request-body",
            )

        mock_original_urlopen.assert_called_once()
        output = mock_stderr.getvalue()
        self.assertIn("Request: GET http://localhost:8080/api/management/v1/catalogs", output)
        self.assertIn("Headers:", output)
        self.assertIn("Body:", output)

    @patch("apache_polaris.cli.polaris_cli.Command.from_options")
    @patch("apache_polaris.cli.polaris_cli.PolarisDefaultApi")
    @patch("apache_polaris.cli.polaris_cli.ApiClientBuilder")
    @patch("apache_polaris.cli.polaris_cli.Parser.parse")
    def test_oauth_request_logged_during_client_creation(
        self,
        mock_parse: MagicMock,
        mock_builder: MagicMock,
        mock_default_api: MagicMock,
        mock_from_options: MagicMock,
    ) -> None:
        mock_parse.return_value = self._debug_options(debug=True)
        mock_builder_instance = MagicMock()
        mock_builder.return_value = mock_builder_instance
        mock_default_api.return_value = MagicMock()
        mock_from_options.return_value = MagicMock()
        oauth_url = "http://localhost:8080/api/catalog/v1/oauth/tokens"
        oauth_body = (
            b"grant_type=client_credentials&client_id=id&client_secret=secret&scope=PRINCIPAL_ROLE:ALL"
        )

        def get_api_client_with_oauth_request() -> MagicMock:
            mock_response = MagicMock()
            with patch.object(
                urllib3.PoolManager,
                "original_urlopen",
                return_value=mock_response,
                create=True,
            ):
                pool = urllib3.PoolManager()
                pool.urlopen("POST", oauth_url, body=oauth_body)
            return MagicMock()

        mock_builder_instance.get_api_client.side_effect = (
            get_api_client_with_oauth_request
        )

        with patch("apache_polaris.cli.polaris_cli.sys.stderr", new_callable=io.StringIO) as mock_stderr:
            PolarisCli.execute(
                [
                    "--debug",
                    "--client-id",
                    "id",
                    "--client-secret",
                    "secret",
                    "catalogs",
                    "list",
                ]
            )

        output = mock_stderr.getvalue()
        self.assertIn(f"Request: POST {oauth_url}", output)
        self.assertIn(f"Body: {oauth_body}", output)


if __name__ == "__main__":
    unittest.main()
