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
from types import SimpleNamespace
from unittest.mock import call, patch, MagicMock, mock_open
from cli_test_utils import CLITestBase, INVALID_ARGS
from apache_polaris.cli.command.setup import SetupCommand
from apache_polaris.cli.constants import Subcommands, UNIT_SEPARATOR
from apache_polaris.cli.exceptions import CliError, CLI_ERROR_EXIT_CODE
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
        read_data=(
            "principal_roles:\n"
            "  - quickstart_user_role\n"
            "principals:\n"
            "  quickstart_user:\n"
            "    roles:\n"
            "      - quickstart_user_role"
        ),
    )
    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_apply_dry_run(
        self, mock_isfile: MagicMock, mock_file: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        self.mock_execute(mock_client, ["setup", "apply", "config.yaml", "--dry-run"])
        mock_client.list_principals.assert_called()

    def test_setup_apply_succeeds_without_failures(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.list_principal_roles.return_value.roles = []
        command = SetupCommand(
            setup_subcommand=Subcommands.APPLY,
            setup_config="config.yaml",
            _config_cache={"principal_roles": ["successful-role"]},
        )

        command.execute(mock_client)

        self.assertEqual(command._failure_count, 0)
        mock_client.create_principal_role.assert_called_once()

    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_dry_run_reports_lookup_failures(
        self, mock_isfile: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        mock_client.list_principals.side_effect = RuntimeError("listing unavailable")
        setup_yaml = "principals:\n  quickstart_user: {}"

        with (
            patch(
                "apache_polaris.cli.command.setup.open",
                mock_open(read_data=setup_yaml),
            ),
            self.assertRaises(CliError) as cm,
        ):
            self.mock_execute(
                mock_client,
                ["setup", "apply", "config.yaml", "--dry-run"],
            )

        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        self.assertIn("setup errors: 1", str(cm.exception))
        mock_client.create_principal.assert_not_called()

    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_dry_run_reports_missing_role_assignments(
        self, mock_isfile: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        setup_yaml = "principals:\n  user:\n    roles:\n      - missing-role"

        with (
            patch(
                "apache_polaris.cli.command.setup.open",
                mock_open(read_data=setup_yaml),
            ),
            self.assertRaises(CliError) as cm,
        ):
            self.mock_execute(
                mock_client,
                ["setup", "apply", "config.yaml", "--dry-run"],
            )

        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        self.assertIn("setup errors: 1", str(cm.exception))

    def test_setup_dry_run_ignores_missing_catalog_roles(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.list_catalog_roles.side_effect = RuntimeError("(404)")
        command = SetupCommand(
            setup_subcommand=Subcommands.APPLY,
            dry_run=True,
        )

        self.assertEqual(
            command._get_existing_catalog_roles(mock_client, "new-catalog"), set()
        )
        self.assertEqual(command._failure_count, 0)

    @patch("apache_polaris.cli.command.setup.PolicyAPI")
    def test_setup_dry_run_reports_policy_lookup_failures(
        self, mock_policy_api: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_policy_api.return_value.load_policy.side_effect = RuntimeError(
            "listing unavailable"
        )
        command = SetupCommand(
            setup_subcommand=Subcommands.APPLY,
            dry_run=True,
        )

        command._create_policies_and_attachments(
            mock_client,
            "catalog",
            {
                "policy": {
                    "namespace": "namespace",
                    "type": "data-compaction",
                    "content": {},
                }
            },
            dry_run=True,
        )

        self.assertEqual(command._failure_count, 1)

    @patch("apache_polaris.cli.command.setup.PolicyAPI")
    def test_setup_apply_recovers_policy_lookup_failure(
        self, mock_policy_api: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_policy_api.return_value.load_policy.side_effect = RuntimeError(
            "listing unavailable"
        )
        command = SetupCommand(
            setup_subcommand=Subcommands.APPLY,
            dry_run=False,
        )

        command._create_policies_and_attachments(
            mock_client,
            "catalog",
            {
                "policy": {
                    "namespace": "namespace",
                    "type": "data-compaction",
                    "content": {},
                }
            },
            dry_run=False,
        )

        self.assertEqual(command._failure_count, 0)
        mock_policy_api.return_value.create_policy.assert_called_once()

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
        mock_client.list_principals.assert_not_called()
        mock_client.list_principal_roles.assert_not_called()
        mock_client.list_catalog_roles.assert_not_called()

    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_apply_reports_missing_external_catalog_connection(
        self, mock_isfile: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        setup_yaml = "catalogs:\n  - name: broken\n    type: external"

        with (
            patch(
                "apache_polaris.cli.command.setup.open",
                mock_open(read_data=setup_yaml),
            ),
            self.assertRaises(CliError) as cm,
        ):
            self.mock_execute(mock_client, ["setup", "apply", "config.yaml"])

        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        self.assertIn("setup errors: 1", str(cm.exception))
        mock_client.create_catalog.assert_not_called()

    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_apply_reports_provisioning_failures(
        self, mock_isfile: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        mock_client.list_principal_roles.side_effect = RuntimeError(
            "listing unavailable"
        )
        mock_client.create_principal_role.side_effect = [
            RuntimeError("backend unavailable"),
            None,
        ]
        setup_yaml = "principal_roles:\n  - failed-role\n  - successful-role"

        with (
            patch(
                "apache_polaris.cli.command.setup.open",
                mock_open(read_data=setup_yaml),
            ),
            self.assertRaises(CliError) as cm,
        ):
            self.mock_execute(mock_client, ["setup", "apply", "config.yaml"])

        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        self.assertIn("setup errors: 2", str(cm.exception))
        self.assertEqual(mock_client.create_principal_role.call_count, 2)

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

    @patch("apache_polaris.cli.command.setup.os.path.isfile")
    def test_setup_apply_treats_null_type_as_internal(
        self, mock_isfile: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_isfile.return_value = True
        mock_client.list_principals.side_effect = RuntimeError("listing unavailable")
        setup_yaml = (
            "catalogs:\n"
            "  - name: my_catalog\n"
            "    type: null\n"
            "    storage_type: file\n"
            "    default_base_location: file:///path"
        )

        with (
            patch(
                "apache_polaris.cli.command.setup.open",
                mock_open(read_data=setup_yaml),
            ),
        ):
            self.mock_execute(
                mock_client,
                ["setup", "apply", "config.yaml"]
            )
        call_args = mock_client.create_catalog.call_args[0][0]
        self.assertEqual(call_args.catalog.type, "INTERNAL")

    @patch("apache_polaris.cli.command.setup.PolicyAPI")
    @patch("apache_polaris.cli.command.setup.IcebergCatalogAPI")
    def test_setup_export_includes_nested_namespaces_and_policies(
        self,
        mock_catalog_api_class: MagicMock,
        mock_policy_api_class: MagicMock,
    ) -> None:
        catalog_api = mock_catalog_api_class.return_value

        def list_namespaces(prefix: str, parent: str | None = None) -> SimpleNamespace:
            self.assertEqual(prefix, "catalog")
            namespaces = {
                None: [["parent"]],
                "parent": [["parent", "child"]],
                f"parent{UNIT_SEPARATOR}child": [],
            }
            return SimpleNamespace(namespaces=namespaces[parent])

        catalog_api.list_namespaces.side_effect = list_namespaces
        catalog_api.load_namespace_metadata.return_value = object()

        policy_api = mock_policy_api_class.return_value
        policy_api.list_policies.side_effect = (
            lambda prefix, namespace: SimpleNamespace(
                identifiers=(
                    [SimpleNamespace(name="child-policy")]
                    if namespace == f"parent{UNIT_SEPARATOR}child"
                    else []
                )
            )
        )
        policy_api.load_policy.return_value = SimpleNamespace(
            policy=SimpleNamespace(
                content='{"max-age": 7}',
                policy_type="data-compaction",
                description=None,
            )
        )

        command = SetupCommand(
            setup_subcommand=Subcommands.EXPORT,
            _catalog_api=MagicMock(),
        )

        self.assertEqual(
            command._export_namespaces_for_catalog(MagicMock(), "catalog"),
            ["parent", "parent.child"],
        )
        self.assertEqual(
            command._export_policies_for_catalog(MagicMock(), "catalog"),
            {
                "child-policy": {
                    "namespace": "parent.child",
                    "type": "data-compaction",
                    "content": '{"max-age":7}',
                }
            },
        )
        policy_api.list_policies.assert_has_calls(
            [
                call(prefix="catalog", namespace="parent"),
                call(
                    prefix="catalog",
                    namespace=f"parent{UNIT_SEPARATOR}child",
                ),
            ]
        )
