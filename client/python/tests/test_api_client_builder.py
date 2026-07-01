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

import json
import unittest
from argparse import Namespace
from unittest.mock import MagicMock, patch

from apache_polaris.cli.api_client_builder import ApiClientBuilder, BuilderConfig
from apache_polaris.cli.exceptions import CliError, CLI_ERROR_EXIT_CODE


def _builder_options() -> Namespace:
    return Namespace(
        base_url="http://localhost:8181",
        catalog_url=None,
        host=None,
        port=None,
        proxy=None,
        access_token=None,
        client_id="test-id",
        client_secret="test-secret",
        realm=None,
        header=None,
        profile=None,
    )


class TestGetToken(unittest.TestCase):
    def _call_get_token(self, response_body: str) -> str:
        builder = ApiClientBuilder(_builder_options(), direct_authentication=True)
        mock_response = MagicMock()
        mock_response.response.data = response_body

        with patch("apache_polaris.cli.api_client_builder.ApiClient") as mock_api_client:
            mock_api_client.return_value.call_api.return_value = mock_response
            return builder._get_token()

    def test_success_returns_access_token(self) -> None:
        token = self._call_get_token(json.dumps({"access_token": "abc123"}))
        self.assertEqual(token, "abc123")

    def test_oauth_error_with_description(self) -> None:
        response_body = json.dumps(
            {
                "error": "invalid_client",
                "error_description": "Client authentication failed",
            }
        )
        with self.assertRaises(CliError) as cm:
            self._call_get_token(response_body)
        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        message = str(cm.exception)
        self.assertIn("Failed to get access token", message)
        self.assertIn("invalid_client", message)
        self.assertIn("Client authentication failed", message)

    def test_oauth_error_without_description(self) -> None:
        response_body = json.dumps({"error": "invalid_grant"})
        with self.assertRaises(CliError) as cm:
            self._call_get_token(response_body)
        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        message = str(cm.exception)
        self.assertIn("Failed to get access token", message)
        self.assertIn("invalid_grant", message)

    def test_missing_token_and_oauth_error_fields(self) -> None:
        with self.assertRaises(CliError) as cm:
            self._call_get_token(json.dumps({}))
        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        self.assertEqual(str(cm.exception), "Failed to get access token")

    def test_invalid_json_response(self) -> None:
        with self.assertRaises(json.JSONDecodeError):
            self._call_get_token("not-json")

    @patch("apache_polaris.cli.api_client_builder.json.loads")
    def test_response_body_parsed_once(self, mock_loads: MagicMock) -> None:
        mock_loads.return_value = {"access_token": "abc123"}
        builder = ApiClientBuilder(_builder_options(), direct_authentication=True)
        mock_response = MagicMock()
        mock_response.response.data = '{"access_token": "abc123"}'

        with patch("apache_polaris.cli.api_client_builder.ApiClient") as mock_api_client:
            mock_api_client.return_value.call_api.return_value = mock_response
            token = builder._get_token()

        self.assertEqual(token, "abc123")
        mock_loads.assert_called_once_with('{"access_token": "abc123"}')


class TestBuilderConfigCatalogUrl(unittest.TestCase):
    def _options(self, *, catalog_url: str | None = None) -> Namespace:
        return Namespace(
            base_url="http://localhost:8181",
            catalog_url=catalog_url,
            host=None,
            port=None,
            proxy=None,
            access_token=None,
            client_id=None,
            client_secret=None,
            realm=None,
            header=None,
            profile=None,
        )

    def test_explicit_catalog_url_when_provided(self) -> None:
        opts = self._options(catalog_url="http://localhost:8181/server1")
        cfg = BuilderConfig(opts)
        self.assertEqual(cfg.explicit_catalog_url, "http://localhost:8181/server1")
        self.assertEqual(cfg.catalog_url, "http://localhost:8181/server1")

    def test_catalog_url_falls_back_when_not_provided(self) -> None:
        opts = self._options(catalog_url=None)
        cfg = BuilderConfig(opts)
        self.assertIsNone(cfg.explicit_catalog_url)
        self.assertEqual(cfg.catalog_url, "http://localhost:8181/api/catalog")

    def test_explicit_catalog_url_set_on_client(self) -> None:
        opts = self._options(catalog_url="http://localhost:8181/server1")
        opts.client_id = "id"
        opts.client_secret = "secret"
        builder = ApiClientBuilder(opts, direct_authentication=True)
        with patch("apache_polaris.cli.api_client_builder.ApiClient") as mock_api:
            mock_client = MagicMock()
            mock_api.return_value = mock_client
            builder.get_api_client()
            self.assertTrue(hasattr(mock_client.configuration, "_polaris_catalog_base"))
            self.assertEqual(
                mock_client.configuration._polaris_catalog_base,
                "http://localhost:8181/server1",
            )

    def test_no_explicit_when_not_set(self) -> None:
        opts = self._options(catalog_url=None)
        opts.client_id = "id"
        opts.client_secret = "secret"
        builder = ApiClientBuilder(opts, direct_authentication=True)
        with patch("apache_polaris.cli.api_client_builder.ApiClient") as mock_api:
            # Use plain object so we can assert attr was never set
            mock_config = type("obj", (object,), {})()
            mock_client = MagicMock()
            mock_client.configuration = mock_config
            mock_api.return_value = mock_client
            builder.get_api_client()
            self.assertFalse(hasattr(mock_config, "_polaris_catalog_base"))


if __name__ == "__main__":
    unittest.main()
