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
from typing import Any, cast
from unittest.mock import patch, MagicMock
from cli_test_utils import CLITestBase, INVALID_ARGS
from apache_polaris.cli.options.parser import Parser


class TestFindCommand(CLITestBase):
    def test_find_basic(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.list_principals.return_value.principals = []
        mock_client.list_principal_roles.return_value.roles = []
        mock_client.list_catalogs.return_value.catalogs = []
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["find", "my-entity"])
            output = mock_stdout.getvalue()
            self.assertIn("Searching for 'my-entity'...", output)
            self.assertIn("No identifiers found matching 'my-entity'.", output)
        mock_client.list_principals.assert_called()
        mock_client.list_principal_roles.assert_called()
        mock_client.list_catalogs.assert_called()

    @patch("apache_polaris.cli.command.find.IcebergCatalogAPI")
    def test_find_with_filters(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_catalog = MagicMock()
        mock_catalog.name = "my-catalog"
        mock_client.get_catalog.return_value = mock_catalog
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.list_namespaces.return_values.namespaces = []
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(
                mock_client,
                ["find", "my_table", "--catalog", "my-catalog", "--type", "table"],
            )
            output = mock_stdout.getvalue()
            self.assertIn("Searching for 'my_table'...", output)
        mock_client.list_principals.assert_not_called()
        mock_client.get_catalog.assert_called_with("my-catalog")
        mock_iceberg_api.list_namespaces.assert_called_with(prefix="my-catalog")

    def test_find_global_entities(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(mock_client, ["find", "admin", "--type", "principal"])
        mock_client.list_principals.assert_called()
        mock_client.list_principal_roles.assert_not_called()

    @patch("apache_polaris.cli.command.find.IcebergCatalogAPI")
    def test_find_dotted_identifier(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_catalog = MagicMock()
        mock_catalog.name = "test-catalog"
        mock_client.list_catalogs.return_value.catalogs = [mock_catalog]
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(mock_client, ["find", "ns1.ns2.my_tabe"])
        mock_client.list_catalogs.assert_called()
        mock_iceberg_api.list_tables.assert_called()
        mock_iceberg_api.list_views.assert_called()

    def test_find_catalog_nonexistent(self) -> None:
        mock_client = self.build_mock_client()
        error = Exception("Not Found")
        any_error = cast(Any, error)
        any_error.status = 404
        mock_client.get_catalog.side_effect = any_error
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["find", "my_table", "--catalog", "missing"])
            output = mock_stdout.getvalue()
            self.assertIn("Catalog 'missing' not found.", output)
        mock_client.get_catalog.assert_called_once_with("missing")

    def test_find_valid_type(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["find", "my-entity", "--type", "invalid-type"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

    def test_find_summary(self) -> None:
        mock_client = self.build_mock_client()
        mock_principal = MagicMock()
        mock_principal.name = "my-entity"
        mock_client.list_principals.return_value.principals = [mock_principal]
        mock_client.list_principal_roles.return_values.roles = []
        mock_client.list_catalogs.return_value.catalogs = []
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["find", "my-entity"])
            output = mock_stdout.getvalue()
            self.assertIn("Found 1 matches (1 Principal).", output)
