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
from apache_polaris.cli.constants import UNIT_SEPARATOR
from apache_polaris.cli.exceptions import CLI_ERROR_EXIT_CODE
from apache_polaris.cli.polaris_cli import PolarisCli
from apache_polaris.sdk.catalog.exceptions import ApiException
from apache_polaris.sdk.catalog.models import (
    LoadViewResult,
    ModelSchema,
    SQLViewRepresentation,
    StructField,
    Type,
    ViewHistoryEntry,
    ViewMetadata,
    ViewRepresentation,
    ViewVersion,
)


def _build_load_view_result(current_version_id: int = 1) -> LoadViewResult:
    schema = ModelSchema(
        type="struct",
        schema_id=0,
        fields=[
            StructField(
                id=1, name="id", type=Type("long"), required=True, doc="primary key"
            ),
            StructField(id=2, name="name", type=Type("string"), required=False),
        ],
    )
    version = ViewVersion(
        version_id=1,
        timestamp_ms=1700000000000,
        schema_id=0,
        summary={},
        default_namespace=["ns1"],
        representations=[
            ViewRepresentation(
                SQLViewRepresentation(
                    type="sql", sql="SELECT id FROM t", dialect="spark"
                )
            )
        ],
    )
    metadata = ViewMetadata(
        view_uuid="deadbeef-dead-beef-dead-beefdeadbeef",
        format_version=1,
        location="s3://bucket/ns1.db/my_view",
        current_version_id=current_version_id,
        versions=[version],
        version_log=[ViewHistoryEntry(version_id=1, timestamp_ms=1700000000000)],
        schemas=[schema],
        properties={},
    )
    return LoadViewResult(
        metadata_location="s3://bucket/ns1.db/my_view/v1.metadata.json",
        metadata=metadata,
    )


class TestViewsCommand(CLITestBase):
    def test_view_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing catalog/namespace
        self.check_exception(
            lambda: self.mock_execute(mock_client, ["views", "list"]),
            "Missing required argument",
        )
        # Empty view name
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                ["views", "get", " ", "--catalog", "my-catalog", "--namespace", "ns1"],
            ),
            "The view name cannot be empty",
        )

    @patch("apache_polaris.cli.command.views.IcebergCatalogAPI")
    def test_view_list(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.list_views.return_value.identifiers = []
        self.mock_execute(
            mock_client,
            ["views", "list", "--catalog", "my-catalog", "--namespace", "ns1.ns2"],
        )
        mock_iceberg_api.list_views.assert_called_once_with(
            prefix="my-catalog", namespace=UNIT_SEPARATOR.join(["ns1", "ns2"])
        )

    @patch("apache_polaris.cli.command.views.IcebergCatalogAPI")
    def test_view_get(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.load_view.return_value.to_json.return_value = "{}"
        self.mock_execute(
            mock_client,
            [
                "views",
                "get",
                "my_view",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_iceberg_api.load_view.assert_called_once_with(
            prefix="my-catalog", namespace="ns1", view="my_view"
        )

    @patch("apache_polaris.cli.command.views.IcebergCatalogAPI")
    def test_view_delete(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        self.mock_execute(
            mock_client,
            [
                "views",
                "delete",
                "my_view",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
            ],
        )
        mock_iceberg_api.drop_view.assert_called_once_with(
            prefix="my-catalog",
            namespace="ns1",
            view="my_view",
        )

    @patch("apache_polaris.cli.polaris_cli.PolarisCli.print_api_exception")
    @patch("apache_polaris.cli.command.views.IcebergCatalogAPI")
    @patch("apache_polaris.cli.polaris_cli.PolarisDefaultApi")
    @patch("apache_polaris.cli.polaris_cli.ApiClientBuilder.get_api_client")
    def test_view_api_errors_exit_with_runtime_failure(
        self,
        _mock_get_api_client: MagicMock,
        mock_default_api: MagicMock,
        mock_iceberg_api_class: MagicMock,
        mock_print_api_exception: MagicMock,
    ) -> None:
        mock_default_api.return_value = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        cases = [
            ("list_views", ["views", "list"]),
            ("load_view", ["views", "get", "my_view"]),
            ("drop_view", ["views", "delete", "my_view"]),
        ]

        for api_method, args in cases:
            with self.subTest(api_method=api_method):
                method = getattr(mock_iceberg_api, api_method)
                method.side_effect = ApiException(status=404, reason="Not Found")

                with self.assertRaises(SystemExit) as cm:
                    PolarisCli.execute(
                        [*args, "--catalog", "my-catalog", "--namespace", "ns1"]
                    )

                self.assertEqual(cm.exception.code, CLI_ERROR_EXIT_CODE)
                mock_print_api_exception.assert_called_once()
                method.side_effect = None
                mock_print_api_exception.reset_mock()

    @patch("apache_polaris.cli.command.views.IcebergCatalogAPI")
    def test_view_summarize(self, mock_iceberg_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.load_view.return_value = _build_load_view_result()

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(
                mock_client,
                [
                    "views",
                    "summarize",
                    "my_view",
                    "--catalog",
                    "my-catalog",
                    "--namespace",
                    "ns1",
                ],
            )
        output = mock_stdout.getvalue()
        mock_iceberg_api.load_view.assert_called_once_with(
            prefix="my-catalog", namespace="ns1", view="my_view"
        )
        self.assertIn("View: ns1.my_view", output)
        self.assertIn("Location:", output)
        self.assertIn("s3://bucket/ns1.db/my_view", output)
        self.assertIn("Format Version:", output)
        self.assertIn("Current Version ID:", output)
        self.assertIn("Last Updated:", output)
        self.assertIn("2023-11-14 22:13:20 UTC", output)
        self.assertIn("Dialect:", output)
        self.assertIn("spark", output)
        self.assertIn("SELECT id FROM t", output)
        self.assertIn("id", output)
        self.assertIn("long", output)
        self.assertIn("primary key", output)
        self.assertIn("string", output)
        self.assertIn("Version History", output)
        self.assertNotIn("No matching version found", output)

    @patch("apache_polaris.cli.command.views.IcebergCatalogAPI")
    def test_view_summarize_reports_missing_current_version(
        self, mock_iceberg_api_class: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api.load_view.return_value = _build_load_view_result(
            current_version_id=99
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(
                mock_client,
                [
                    "views",
                    "summarize",
                    "my_view",
                    "--catalog",
                    "my-catalog",
                    "--namespace",
                    "ns1",
                ],
            )
        output = mock_stdout.getvalue()
        self.assertIn(
            "No matching version found for the current version ID", output
        )
        self.assertNotIn("Version History", output)
