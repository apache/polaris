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
from apache_polaris.sdk.management import (
    PolarisCatalog,
    CatalogProperties,
    AwsStorageConfigInfo,
)


class TestCatalogsCommand(CLITestBase):
    def test_catalog_commands_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing storage type
        self.check_exception(
            lambda: self.mock_execute(
                mock_client, ["catalogs", "create", "my-catalog"]
            ),
            "--storage-type",
        )
        # Missing default base location
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                ["catalogs", "create", "my-catalog", "--storage-type", "gcs"],
            ),
            "--default-base-location",
        )
        # Invalid property format
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                ["catalogs", "update", "foo", "--set-property", "bad-format"],
            ),
            "bad-format",
        )
        # Invalid options for storage type
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "gcs",
                    "--allowed-location",
                    "a",
                    "--allowed-location",
                    "b",
                    "--role-arn",
                    "ra",
                    "--default-base-location",
                    "x",
                ],
            ),
            "gcs",
        )
        # Missing required argument for connection type 'hive'
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--type",
                    "external",
                    "--storage-type",
                    "file",
                    "--default-base-location",
                    "dbl",
                    "--catalog-connection-type",
                    "hive",
                    "--catalog-authentication-type",
                    "implicit",
                ],
            ),
            "--hive-warehouse",
        )
        # Authentication type 'OAUTH' requires additional fields
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--type",
                    "external",
                    "--catalog-connection-type",
                    "iceberg-rest",
                    "--catalog-authentication-type",
                    "oauth",
                    "--catalog-uri",
                    "u",
                ],
            ),
            "Authentication type 'OAUTH' requires",
        )
        # Authentication type 'BEARER' requires bearer token
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--type",
                    "external",
                    "--catalog-connection-type",
                    "iceberg-rest",
                    "--catalog-authentication-type",
                    "bearer",
                    "--catalog-uri",
                    "u",
                ],
            ),
            "Missing required argument for authentication type 'BEARER'",
        )

    def test_catalog_create_s3_options(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "s3-catalog",
                "--storage-type",
                "s3",
                "--default-base-location",
                "s3://bucket/path",
                "--role-arn",
                "arn:aws:iam::123456789012:role/my-role",
                "--no-sts",
                "--no-kms",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "s3-catalog")
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "S3")
        self.assertEqual(
            call_args.catalog.storage_config_info.role_arn,
            "arn:aws:iam::123456789012:role/my-role",
        )
        self.assertTrue(call_args.catalog.storage_config_info.sts_unavailable)
        self.assertTrue(call_args.catalog.storage_config_info.kms_unavailable)

        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "s3",
                "--allowed-location",
                "a",
                "--allowed-location",
                "b",
                "--role-arn",
                "ra",
                "--external-id",
                "ei",
                "--default-base-location",
                "x",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "S3")
        self.assertEqual(call_args.catalog.properties.default_base_location, "x")
        self.assertEqual(
            call_args.catalog.storage_config_info.allowed_locations, ["a", "b"]
        )

        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "s3",
                "--allowed-location",
                "a",
                "--role-arn",
                "ra",
                "--region",
                "us-west-2",
                "--external-id",
                "ei",
                "--default-base-location",
                "x",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "S3")
        self.assertEqual(call_args.catalog.properties.default_base_location, "x")
        self.assertEqual(call_args.catalog.storage_config_info.allowed_locations, ["a"])
        self.assertEqual(call_args.catalog.storage_config_info.region, "us-west-2")

    def test_catalog_create_gcs_options(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "gcs",
                "--default-base-location",
                "x",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "GCS")
        self.assertEqual(call_args.catalog.properties.default_base_location, "x")

        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "gcs",
                "--allowed-location",
                "a",
                "--allowed-location",
                "b",
                "--service-account",
                "sa",
                "--default-base-location",
                "x",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "GCS")
        self.assertEqual(call_args.catalog.properties.default_base_location, "x")
        self.assertEqual(
            call_args.catalog.storage_config_info.allowed_locations, ["a", "b"]
        )
        self.assertEqual(
            call_args.catalog.storage_config_info.gcs_service_account, "sa"
        )

    def test_catalog_create_azure_options(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "azure",
                "--default-base-location",
                "abfss://container@account.blob.core.windows.net/path",
                "--tenant-id",
                "my-tenant-id",
                "--hierarchical",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "AZURE")
        self.assertEqual(
            call_args.catalog.properties.default_base_location,
            "abfss://container@account.blob.core.windows.net/path",
        )
        self.assertEqual(
            call_args.catalog.storage_config_info.tenant_id, "my-tenant-id"
        )
        self.assertEqual(call_args.catalog.storage_config_info.hierarchical, True)

    def test_catalog_create_external_options(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "gcs",
                "--default-base-location",
                "dbl",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")

        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "gcs",
                "--default-base-location",
                "dbl",
                "--catalog-connection-type",
                "iceberg-rest",
                "--iceberg-remote-catalog-name",
                "i",
                "--catalog-uri",
                "u",
                "--catalog-authentication-type",
                "bearer",
                "--catalog-bearer-token",
                "b",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")
        self.assertEqual(
            call_args.catalog.connection_config_info.connection_type, "ICEBERG_REST"
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.remote_catalog_name, "i"
        )
        self.assertEqual(call_args.catalog.connection_config_info.uri, "u")

    def test_catalog_storage_type_exclusivity(self) -> None:
        mock_client = self.build_mock_client()
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "catalogs",
                    "create",
                    "bad-catalog",
                    "--storage-type",
                    "azure",
                    "--default-base-location",
                    "abfss://container@account.blob.core.windows.net/path",
                    "--tenant-id",
                    "my-tenant-id",
                    # S3 specific
                    "--role-arn",
                    "arn:aws:iam::123456789012:role/my-role",
                ],
            ),
            "Storage type 'azure' supports the options",
        )

    def test_catalog_name_parsing(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_catalog.return_value = PolarisCatalog(
            type="INTERNAL",
            name="foo",
            entity_version=1,
            properties=CatalogProperties(
                default_base_location="s3://bucket/path", additional_properties={}
            ),
            storage_config_info=AwsStorageConfigInfo(
                storage_type="S3", allowed_locations=[]
            ),
        )
        self.mock_execute(
            mock_client,
            ["catalogs", "update", "foo", "--default-base-location", "x"],
        )
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(
            mock_client,
            ["catalogs", "update", "foo", "--set-property", "key=value"],
        )
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(
            mock_client,
            ["catalogs", "update", "foo", "--set-property", "listkey=k1=v1,k2=v2"],
        )
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(
            mock_client,
            ["catalogs", "update", "foo", "--remove-property", "key"],
        )
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "update",
                "foo",
                "--set-property",
                "key=value",
                "--default-base-location",
                "x",
            ],
        )
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "update",
                "foo",
                "--set-property",
                "key=value",
                "--default-base-location",
                "x",
                "--region",
                "us-west-1",
            ],
        )
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(mock_client, ["catalogs", "delete", "foo"])
        mock_client.get_catalog.assert_called_with("foo")
        self.mock_execute(mock_client, ["catalogs", "get", "foo"])
        mock_client.get_catalog.assert_called_with("foo")

    def test_catalog_list(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.list_catalogs.return_values.catalogs = []
        # Default
        self.mock_execute(mock_client, ["catalogs", "list"])
        mock_client.list_catalogs.assert_called()
        # Custom base-url
        self.mock_execute(
            mock_client,
            ["--base-url", "https://customservice.com/subpath", "catalogs", "list"],
        )
        mock_client.list_catalogs.assert_called()

    def test_external_catalog_oauth(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "file",
                "--default-base-location",
                "dbl",
                "--catalog-connection-type",
                "hive",
                "--hive-warehouse",
                "/warehouse/path",
                "--catalog-authentication-type",
                "oauth",
                "--catalog-uri",
                "thrift://hive-metastore:9083",
                "--catalog-token-uri",
                "http://auth-server/token",
                "--catalog-client-id",
                "test-client",
                "--catalog-client-secret",
                "test-secret",
                "--catalog-client-scope",
                "read",
                "--catalog-client-scope",
                "write",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")
        self.assertEqual(
            call_args.catalog.connection_config_info.connection_type, "HIVE"
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.warehouse, "/warehouse/path"
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.authentication_type,
            "OAUTH",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.uri, "thrift://hive-metastore:9083"
        )

        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "gcs",
                "--default-base-location",
                "dbl",
                "--catalog-connection-type",
                "iceberg-rest",
                "--iceberg-remote-catalog-name",
                "c",
                "--catalog-uri",
                "u",
                "--catalog-authentication-type",
                "oauth",
                "--catalog-token-uri",
                "u",
                "--catalog-client-id",
                "i",
                "--catalog-client-secret",
                "k",
                "--catalog-client-scope",
                "s1",
                "--catalog-client-scope",
                "s2",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")
        self.assertEqual(
            call_args.catalog.connection_config_info.connection_type, "ICEBERG_REST"
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.remote_catalog_name, "c"
        )
        self.assertEqual(call_args.catalog.connection_config_info.uri, "u")
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.authentication_type,
            "OAUTH",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.token_uri,
            "u",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.client_id,
            "i",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.scopes,
            ["s1", "s2"],
        )
        self.assertEqual(call_args.catalog.storage_config_info.storage_type, "GCS")
        self.assertEqual(call_args.catalog.properties.default_base_location, "dbl")

    def test_external_catalog_hadoop(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "file",
                "--default-base-location",
                "dbl",
                "--catalog-connection-type",
                "hadoop",
                "--hadoop-warehouse",
                "h",
                "--catalog-authentication-type",
                "implicit",
                "--catalog-uri",
                "u",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")
        self.assertEqual(
            call_args.catalog.connection_config_info.connection_type, "HADOOP"
        )
        self.assertEqual(call_args.catalog.connection_config_info.warehouse, "h")
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.authentication_type,
            "IMPLICIT",
        )
        self.assertEqual(call_args.catalog.connection_config_info.uri, "u")

    def test_external_catalog_hive(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "file",
                "--default-base-location",
                "dbl",
                "--catalog-connection-type",
                "hive",
                "--hive-warehouse",
                "h",
                "--catalog-authentication-type",
                "implicit",
                "--catalog-uri",
                "u",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")
        self.assertEqual(
            call_args.catalog.connection_config_info.connection_type, "HIVE"
        )
        self.assertEqual(call_args.catalog.connection_config_info.warehouse, "h")
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.authentication_type,
            "IMPLICIT",
        )
        self.assertEqual(call_args.catalog.connection_config_info.uri, "u")

    def test_external_catalog_sigv4(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "gcs",
                "--default-base-location",
                "dbl",
                "--catalog-connection-type",
                "iceberg-rest",
                "--iceberg-remote-catalog-name",
                "i",
                "--catalog-uri",
                "u",
                "--catalog-authentication-type",
                "sigv4",
                "--catalog-role-arn",
                "a",
                "--catalog-signing-region",
                "s",
                "--catalog-role-session-name",
                "n",
                "--catalog-external-id",
                "i",
                "--catalog-signing-name",
                "g",
            ],
        )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.name, "my-catalog")
        self.assertEqual(call_args.catalog.type, "EXTERNAL")
        self.assertEqual(
            call_args.catalog.connection_config_info.connection_type, "ICEBERG_REST"
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.remote_catalog_name, "i"
        )
        self.assertEqual(call_args.catalog.connection_config_info.uri, "u")
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.role_arn,
            "a",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.signing_region,
            "s",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.role_session_name,
            "n",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.external_id,
            "i",
        )
        self.assertEqual(
            call_args.catalog.connection_config_info.authentication_parameters.signing_name,
            "g",
        )

    @patch("apache_polaris.cli.command.catalogs.IcebergCatalogAPI")
    @patch("apache_polaris.cli.command.catalogs.PolicyAPI")
    def test_catalog_summarize(
        self, mock_policy_api_class: MagicMock, mock_iceberg_api_class: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_catalog.return_value = PolarisCatalog(
            type="INTERNAL",
            name="foo",
            entity_version=1,
            properties=CatalogProperties(
                default_base_location="s3://bucket/path", additional_properties={}
            ),
            storage_config_info=AwsStorageConfigInfo(
                storage_type="S3", allowed_locations=[]
            ),
        )
        mock_client.list_catalog_roles.return_value.roles = []
        mock_iceberg_api = mock_iceberg_api_class.return_value
        mock_iceberg_api_class.list_namespaces.return_value.namespaces = []
        mock_policy_api = mock_policy_api_class.return_value
        mock_policy_api.get_applicable_policies.return_value.applicable_policies = []
        self.mock_execute(mock_client, ["catalogs", "summarize", "foo"])
        mock_client.get_catalog.assert_called_with("foo")
        mock_iceberg_api.list_namespaces.assert_called_with(prefix="foo", parent=None)
        mock_policy_api.get_applicable_policies.assert_called_with(prefix="foo")
