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
from unittest.mock import patch, MagicMock, mock_open
from cli_test_utils import CLITestBase, INVALID_ARGS
from apache_polaris.sdk.management import (
    PolarisCatalog,
    CatalogProperties,
    FileStorageConfigInfo,
)


class TestSetupCommand(CLITestBase):
    def test_setup_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing setup config
        with self.assertRaises(SystemExit) as cm:
            with patch("sys.stderr", new_callable=io.StringIO):
                self.mock_execute(mock_client, ["setup", "apply"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

    @patch(
        "apache_polaris.cli.command.setup.open",
        new_callable=mock_open,
        read_data="principals:\n  quickstart_user:\n    roles:\n      - quickstart_user_role",
    )
    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_apply_dry_run(
        self, mock_isfile: MagicMock, mock_file: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        self.mock_execute(mock_client, ["setup", "apply", "config.yaml", "--dry-run"])
        mock_client.list_principals.assert_called()

    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_apply_s3_optional_fields(self, mock_isfile: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        # Sample S3 catalog without role_arn
        setup_yaml = "catalogs:\n  - name: s3-catalog\n    storage_type: s3\n    default_base_location: s3://bucket/path"
        with patch(
            "apache_polaris.cli.command.setup.open", mock_open(read_data=setup_yaml)
        ):
            self.mock_execute(mock_client, ["setup", "apply", "config.yaml"])
        mock_client.create_catalog.assert_called_once()
        call_args = mock_client.create_catalog.call_args[0][0]
        # role_arn should be None, NOT an empty string
        self.assertIsNone(call_args.catalog.storage_config_info.role_arn)
        self.assertEqual(call_args.catalog.name, "s3-catalog")

    @patch(
        "apache_polaris.cli.command.setup.open", new_callable=mock_open, read_data="{}"
    )
    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_export(self, mock_isfile: MagicMock, mock_file: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        mock_catalog = MagicMock()
        mock_catalog.name = "my_catalog"
        mock_client.list_catalogs.return_value.catalogs = [mock_catalog]
        mock_client.get_catalog.return_value = PolarisCatalog(
            type="INTERNAL",
            name="my_catalog",
            entity_version=1,
            properties=CatalogProperties(
                default_base_location="file:///path",
                additional_properties={},
            ),
            storage_config_info=FileStorageConfigInfo(
                storage_type="FILE",
                allowed_locations=["file:///path"],
            ),
        )
        mock_client.list_catalog_roles.return_value = MagicMock(roles=[])
        self.mock_execute(mock_client, ["setup", "export"])
        mock_client.list_principals.assert_called()
        mock_client.list_principal_roles.assert_called()
        mock_client.list_catalogs.assert_called()
        mock_client.list_catalog_roles.assert_called_with("my_catalog")
        mock_client.get_catalog.assert_called_with("my_catalog")
