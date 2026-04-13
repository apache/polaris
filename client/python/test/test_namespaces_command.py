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


class TestNamespacesCommand(CLITestBase):
    def test_namespace_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing catalog
        self.check_exception(
            lambda: self.mock_execute(mock_client, ["namespaces", "list"]),
            "Missing required argument",
        )

    @patch("apache_polaris.cli.command.namespaces.IcebergCatalogAPI")
    def test_namespace_create(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(
            mock_client,
            [
                "namespaces",
                "create",
                "ns1.ns2",
                "--catalog",
                "my-catalog",
                "--location",
                "s3://bucket/path",
                "--property",
                "k=v",
            ],
        )
        mock_iceberg_api.create_namespace.assert_called_once()
        _, kwargs = mock_iceberg_api.create_namespace.call_args
        self.assertEqual(kwargs["prefix"], "my-catalog")
        self.assertEqual(kwargs["create_namespace_request"].namespace, ["ns1", "ns2"])
        self.assertEqual(
            kwargs["create_namespace_request"].properties,
            {"k": "v", "location": "s3://bucket/path"},
        )

    @patch("apache_polaris.cli.command.namespaces.IcebergCatalogAPI")
    def test_namespace_list(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(
            mock_client, ["namespaces", "list", "--catalog", "my-catalog"]
        )
        mock_iceberg_api.list_namespaces.assert_called_with(prefix="my-catalog")

        self.mock_execute(
            mock_client,
            ["namespaces", "list", "--catalog", "my-catalog", "--parent", "ns1"],
        )
        mock_iceberg_api.list_namespaces.assert_called_with(
            prefix="my-catalog", parent="ns1"
        )

    @patch("apache_polaris.cli.command.namespaces.IcebergCatalogAPI")
    def test_namespace_delete(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(
            mock_client, ["namespaces", "delete", "ns1.ns2", "--catalog", "my-catalog"]
        )
        mock_iceberg_api.drop_namespace.assert_called_once_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )

    @patch("apache_polaris.cli.command.namespaces.IcebergCatalogAPI")
    def test_namespace_get(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(
            mock_client, ["namespaces", "get", "ns1.ns2", "--catalog", "my-catalog"]
        )
        mock_iceberg_api.load_namespace_metadata.assert_called_once_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )

    @patch("apache_polaris.cli.command.namespaces.PolicyAPI")
    @patch("apache_polaris.cli.command.namespaces.IcebergCatalogAPI")
    def test_namespace_summarize(
        self, mock_iceberg_api_class: MagicMock, mock_policy_api_class: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_policy_api = mock_policy_api_class.return_value
        self.mock_execute(
            mock_client,
            ["namespaces", "summarize", "ns1.ns2", "--catalog", "my-catalog"],
        )
        mock_iceberg_api.list_namespaces.assert_called_with(
            prefix="my-catalog", parent=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )
        mock_iceberg_api.list_tables.assert_called_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )
        mock_iceberg_api.list_views.assert_called_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )
        mock_policy_api.get_applicable_policies.assert_called_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )
