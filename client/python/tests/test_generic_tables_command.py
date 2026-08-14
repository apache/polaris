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
from apache_polaris.cli.exceptions import CLI_ERROR_EXIT_CODE
from apache_polaris.cli.polaris_cli import PolarisCli
from apache_polaris.sdk.catalog.exceptions import ApiException


class TestGenericTablesCommand(CLITestBase):
    def test_generic_table_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing catalog/namespace
        self.check_exception(
            lambda: self.mock_execute(mock_client, ["generic-tables", "list"]),
            "Missing required argument",
        )
        # Empty generic table name
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "generic-tables",
                    "get",
                    " ",
                    "--catalog",
                    "my-catalog",
                    "--namespace",
                    "ns1",
                ],
            ),
            "The generic table name cannot be empty",
        )

    @patch("apache_polaris.cli.command.generic_tables.GenericTableAPI")
    def test_generic_table_list(self, mock_generic_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_generic_api = mock_generic_api_class.return_value
        mock_generic_api.list_generic_tables.return_value.identifiers = []
        self.mock_execute(
            mock_client,
            [
                "generic-tables",
                "list",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1.ns2",
            ],
        )
        mock_generic_api.list_generic_tables.assert_called_once_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )

    @patch("apache_polaris.cli.command.generic_tables.GenericTableAPI")
    def test_generic_table_get(self, mock_generic_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_generic_api = mock_generic_api_class.return_value
        mock_generic_api.load_generic_table.return_value.to_json.return_value = "{}"
        self.mock_execute(
            mock_client,
            [
                "generic-tables",
                "get",
                "my_table",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_generic_api.load_generic_table.assert_called_once_with(
            prefix="my-catalog", namespace="ns1", generic_table="my_table"
        )

    @patch("apache_polaris.cli.command.generic_tables.GenericTableAPI")
    def test_generic_table_delete(self, mock_generic_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_generic_api = mock_generic_api_class.return_value
        self.mock_execute(
            mock_client,
            [
                "generic-tables",
                "delete",
                "my_table",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_generic_api.drop_generic_table.assert_called_once_with(
            prefix="my-catalog",
            namespace="ns1",
            generic_table="my_table",
        )

    @patch("apache_polaris.cli.polaris_cli.PolarisCli.print_api_exception")
    @patch("apache_polaris.cli.command.generic_tables.GenericTableAPI")
    @patch("apache_polaris.cli.polaris_cli.PolarisDefaultApi")
    @patch("apache_polaris.cli.polaris_cli.ApiClientBuilder.get_api_client")
    def test_generic_table_api_errors_exit_with_runtime_failure(
        self,
        _mock_get_api_client: MagicMock,
        mock_default_api: MagicMock,
        mock_generic_api_class: MagicMock,
        mock_print_api_exception: MagicMock,
    ) -> None:
        mock_default_api.return_value = self.build_mock_client()
        mock_generic_api = mock_generic_api_class.return_value
        cases = [
            ("list_generic_tables", ["generic-tables", "list"]),
            ("load_generic_table", ["generic-tables", "get", "my_table"]),
            ("drop_generic_table", ["generic-tables", "delete", "my_table"]),
        ]

        for api_method, args in cases:
            with self.subTest(api_method=api_method):
                method = getattr(mock_generic_api, api_method)
                method.side_effect = ApiException(status=404, reason="Not Found")

                with self.assertRaises(SystemExit) as cm:
                    PolarisCli.execute(
                        [*args, "--catalog", "my-catalog", "--namespace", "ns1"]
                    )

                self.assertEqual(cm.exception.code, CLI_ERROR_EXIT_CODE)
                mock_print_api_exception.assert_called_once()
                method.side_effect = None
                mock_print_api_exception.reset_mock()
