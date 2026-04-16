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

from unittest.mock import patch, MagicMock
from cli_test_utils import CLITestBase
from apache_polaris.cli.constants import UNIT_SEPARATOR


class TestTablesCommand(CLITestBase):
    def test_table_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing catalog/namespace
        self.check_exception(
            lambda: self.mock_execute(mock_client, ["tables", "list"]),
            "Missing required argument",
        )
        # Empty table name
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                ["tables", "get", " ", "--catalog", "my-catalog", "--namespace", "ns1"],
            ),
            "The table name cannot be empty",
        )

    @patch("apache_polaris.cli.command.tables.IcebergCatalogAPI")
    def test_table_list(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.list_tables.return_value.identifiers = []
        self.mock_execute(
            mock_client,
            ["tables", "list", "--catalog", "my-catalog", "--namespace", "ns1.ns2"],
        )
        mock_iceberg_api.list_tables.assert_called_once_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )

    @patch("apache_polaris.cli.command.tables.IcebergCatalogAPI")
    def test_table_get(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.load_table.return_value.to_json.return_value = "{}"
        self.mock_execute(
            mock_client,
            [
                "tables",
                "get",
                "my_table",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_iceberg_api.load_table.assert_called_once_with(
            prefix="my-catalog", namespace="ns1", table="my_table"
        )

    @patch("apache_polaris.cli.command.tables.IcebergCatalogAPI")
    def test_table_delete(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(
            mock_client,
            [
                "tables",
                "delete",
                "my_table",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_iceberg_api.drop_table.assert_called_once_with(
            prefix="my-catalog",
            namespace="ns1",
            table="my_table",
            purge_requested=False,
        )

    @patch("apache_polaris.cli.command.tables.PolicyAPI")
    @patch("apache_polaris.cli.command.tables.IcebergCatalogAPI")
    def test_table_summarize(
        self, mock_iceberg_api_class: MagicMock, mock_policy_api_class: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_policy_api = mock_policy_api_class.return_value

        self.mock_execute(
            mock_client,
            [
                "tables",
                "summarize",
                "my_table",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_iceberg_api.load_table.assert_called_with(
            prefix="my-catalog", namespace="ns1", table="my_table"
        )
        mock_policy_api.get_applicable_policies.assert_called_with(
            prefix="my-catalog", namespace="ns1", target_name="my_table"
        )
