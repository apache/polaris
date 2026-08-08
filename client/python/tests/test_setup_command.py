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

import yaml

from cli_test_utils import CLITestBase, INVALID_ARGS
from apache_polaris.cli.command.setup import SetupCommand
from apache_polaris.cli.constants import Subcommands, UNIT_SEPARATOR
from apache_polaris.cli.exceptions import CliError, CLI_ERROR_EXIT_CODE
from apache_polaris.sdk.catalog.exceptions import NotFoundException
from apache_polaris.sdk.management import (
    PolarisCatalog,
    CatalogProperties,
    FileStorageConfigInfo,
    AwsStorageConfigInfo,
)
from apache_polaris.sdk.catalog import GetNamespaceResponse


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
        request = mock_policy_api.return_value.create_policy.call_args.kwargs[
            "create_policy_request"
        ]
        self.assertEqual(request.content, "{}")

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
        with patch(
            "apache_polaris.cli.command.setup.IcebergCatalogAPI"
        ) as mock_catalog_api:
            mock_catalog_api.return_value.list_namespaces.return_value.namespaces = []
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
            self.mock_execute(mock_client, ["setup", "apply", "config.yaml"])
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
        catalog_api.load_namespace_metadata.return_value = GetNamespaceResponse(
            namespace=["parent"], properties={}
        )

        policy_api = mock_policy_api_class.return_value
        policy_api.list_policies.side_effect = lambda prefix, namespace: (
            SimpleNamespace(
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
        mock_client = MagicMock()
        mock_client.list_catalogs.return_value = SimpleNamespace(
            catalogs=[SimpleNamespace(name="catalog")]
        )

        mock_client.get_catalog.return_value = PolarisCatalog(
            type="INTERNAL",
            name="catalog",
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
        mock_client.list_catalog_roles.return_value = SimpleNamespace(roles=[])

        catalog = command._export_catalogs(mock_client)[0]

        self.assertEqual(
            catalog["namespaces"], [{"name": "parent"}, {"name": "parent.child"}]
        )
        self.assertEqual(
            catalog["policies"],
            {
                "child-policy": {
                    "namespace": "parent.child",
                    "type": "data-compaction",
                    "content": '{"max-age":7}',
                }
            },
        )
        self.assertEqual(
            catalog_api.list_namespaces.call_args_list,
            [
                call(prefix="catalog"),
                call(prefix="catalog", parent="parent"),
                call(
                    prefix="catalog",
                    parent=f"parent{UNIT_SEPARATOR}child",
                ),
            ],
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

    @patch("apache_polaris.cli.command.setup.PolicyAPI")
    def test_setup_exported_policy_content_round_trips_through_apply(
        self, mock_policy_api_class: MagicMock
    ) -> None:
        policy_content = '{"version":"2025-02-03","enable":true}'
        policy_api = mock_policy_api_class.return_value
        policy_api.list_policies.return_value = SimpleNamespace(
            identifiers=[SimpleNamespace(name="compaction")]
        )
        policy_api.load_policy.side_effect = [
            SimpleNamespace(
                policy=SimpleNamespace(
                    content=policy_content,
                    policy_type="system.data-compaction",
                    description=None,
                )
            ),
            NotFoundException(),
        ]

        export_command = SetupCommand(
            setup_subcommand=Subcommands.EXPORT,
            _catalog_api=MagicMock(),
        )
        exported_policies = export_command._export_policies_for_catalog(
            MagicMock(), "catalog", [["namespace"]]
        )
        loaded_config = yaml.safe_load(yaml.safe_dump({"policies": exported_policies}))

        apply_command = SetupCommand(
            setup_subcommand=Subcommands.APPLY,
            _catalog_api=MagicMock(),
        )
        apply_command._create_policies_and_attachments(
            MagicMock(),
            "catalog",
            loaded_config["policies"],
        )

        request = policy_api.create_policy.call_args.kwargs["create_policy_request"]
        self.assertEqual(request.content, policy_content)

    def test_setup_export_reports_top_level_read_failures(self) -> None:
        for method_name in (
            "list_principals",
            "list_principal_roles",
            "list_catalogs",
        ):
            with self.subTest(method_name=method_name):
                mock_client = self.build_mock_client()
                mock_client.list_principals.return_value.principals = []
                mock_client.list_principal_roles.return_value.roles = []
                mock_client.list_catalogs.return_value.catalogs = []
                getattr(mock_client, method_name).side_effect = RuntimeError(
                    "backend unavailable"
                )
                command = SetupCommand(setup_subcommand=Subcommands.EXPORT)

                with (
                    patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
                    self.assertRaises(CliError) as cm,
                ):
                    command.execute(mock_client)

                self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
                self.assertIn("export errors: 1", str(cm.exception))
                self.assertEqual(mock_stdout.getvalue(), "")

    @patch("apache_polaris.cli.command.setup.IcebergCatalogAPI")
    def test_setup_export_reports_nested_read_failures(
        self, mock_catalog_api: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_principal = MagicMock()
        mock_principal.name = "principal"
        mock_client.list_principals.return_value.principals = [mock_principal]
        mock_client.list_principal_roles_assigned.side_effect = RuntimeError(
            "backend unavailable"
        )
        mock_client.list_principal_roles.return_value.roles = []
        mock_catalog = MagicMock()
        mock_catalog.name = "catalog"
        mock_client.list_catalogs.return_value.catalogs = [mock_catalog]
        mock_client.get_catalog.return_value = PolarisCatalog(
            type="INTERNAL",
            name="catalog",
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
        mock_client.list_catalog_roles.side_effect = RuntimeError("backend unavailable")
        mock_catalog_api.return_value.list_namespaces.side_effect = RuntimeError(
            "backend unavailable"
        )
        command = SetupCommand(
            setup_subcommand=Subcommands.EXPORT,
            _catalog_api=MagicMock(),
        )

        with (
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
            self.assertRaises(CliError) as cm,
        ):
            command.execute(mock_client)

        self.assertEqual(cm.exception.exit_code, CLI_ERROR_EXIT_CODE)
        self.assertIn("export errors: 3", str(cm.exception))
        self.assertEqual(mock_stdout.getvalue(), "")

    @patch("apache_polaris.cli.command.setup.IcebergCatalogAPI")
    def test_setup_export_s3_catalog_round_trips_sts_and_internal_endpoints(
        self, mock_catalog_api: MagicMock
    ) -> None:
        mock_catalog_api.return_value.list_namespaces.return_value = []
        mock_catalog = MagicMock()
        mock_catalog.name = "my_catalog"
        mock_client = self.build_mock_client()
        mock_client.list_catalogs.return_value.catalogs = [mock_catalog]
        mock_client.get_catalog.return_value = PolarisCatalog(
            type="INTERNAL",
            name="my_catalog",
            entity_version=1,
            properties=CatalogProperties(
                default_base_location="s3://bucket/path",
                additional_properties={},
            ),
            storage_config_info=AwsStorageConfigInfo(
                storage_type="S3",
                allowed_locations=["s3://bucket/path"],
                role_arn="arn:aws:iam::123456789012:user/QuickstartUser",
                endpoint="https://s3.us-west-2.amazonaws.com",
                endpoint_internal="https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com",
                sts_endpoint="https://sts.amazonaws.com",
            ),
        )
        mock_client.list_catalog_roles.return_value = MagicMock(roles=[])

        export_command = SetupCommand(
            setup_subcommand=Subcommands.EXPORT,
            _catalog_api=MagicMock(),
        )
        exported = export_command._export_catalogs(mock_client)

        self.assertEqual(len(exported), 1)
        self.assertEqual(
            exported[0]["endpoint_internal"], "https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com"
        )
        self.assertEqual(exported[0]["sts_endpoint"], "https://sts.amazonaws.com")

        loaded = yaml.safe_load(yaml.safe_dump({"catalogs": exported}))

        apply_client = self.build_mock_client()
        apply_client.list_catalogs.return_value.catalogs = []
        apply_command = SetupCommand(setup_subcommand=Subcommands.APPLY)
        apply_command._create_catalogs(apply_client, loaded["catalogs"])

        apply_client.create_catalog.assert_called_once()
        created = apply_client.create_catalog.call_args[0][0].catalog
        self.assertEqual(
            created.storage_config_info.endpoint_internal,
            "https://bucket.vpce-1a2b3c4d-5e6f.s3.us-west-2.vpce.amazonaws.com",
        )
        self.assertEqual(
            created.storage_config_info.sts_endpoint, "https://sts.amazonaws.com"
        )

