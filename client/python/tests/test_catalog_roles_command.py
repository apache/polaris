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

from cli_test_utils import CLITestBase
from apache_polaris.sdk.management import CatalogRole


class TestCatalogRolessCommand(CLITestBase):
    def test_catalog_role_commands_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Missing --catalog flag
        for sub in ["create", "delete", "get", "update", "summarize"]:
            with self.subTest(subcommand=sub):
                self.check_exception(
                    lambda: self.mock_execute(
                        mock_client, ["catalog-roles", sub, "role_name"]
                    ),
                    "--catalog",
                )
        # Missing positional role name
        for sub in ["create", "delete", "get", "update", "summarize"]:
            with self.subTest(subcommand=sub, error="missing positional"):
                with self.assertRaises(SystemExit):
                    self.mock_execute(
                        mock_client, ["catalog-roles", sub, "--catalog", "my_cat"]
                    )

    def test_catalog_role_create(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalog-roles",
                "create",
                "foo",
                "--catalog",
                "bar",
                "--property",
                "key=value",
            ],
        )
        call_args = mock_client.create_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "bar")
        self.assertEqual(call_args[1].catalog_role.name, "foo")
        self.assertEqual(call_args[1].catalog_role.properties, {"key": "value"})

    def test_catalog_role_list(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(mock_client, ["catalog-roles", "list", "foo"])
        call_args = mock_client.list_catalog_roles.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.mock_execute(
            mock_client, ["catalog-roles", "list", "foo", "--principal-role", "bar"]
        )
        call_args = mock_client.list_catalog_roles_for_principal_role.call_args[0]
        self.assertEqual(call_args[0], "bar")
        self.assertEqual(call_args[1], "foo")

    def test_catalog_role_get(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client, ["catalog-roles", "get", "foo", "--catalog", "bar"]
        )
        call_args = mock_client.get_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "bar")
        self.assertEqual(call_args[1], "foo")

    def test_catalog_role_delete(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client, ["catalog-roles", "delete", "foo", "--catalog", "bar"]
        )
        call_args = mock_client.delete_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "bar")
        self.assertEqual(call_args[1], "foo")

    def test_catalog_role_update(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_catalog_role.return_value = CatalogRole(
            name="foo",
            entity_version=1,
        )
        self.mock_execute(
            mock_client,
            [
                "catalog-roles",
                "update",
                "foo",
                "--catalog",
                "bar",
                "--set-property",
                "key=value",
            ],
        )
        call_args = mock_client.update_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "bar")
        self.assertEqual(call_args[1], "foo")

        self.mock_execute(
            mock_client,
            [
                "catalog-roles",
                "update",
                "foo",
                "--catalog",
                "bar",
                "--remove-property",
                "key",
            ],
        )
        call_args = mock_client.update_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "bar")
        self.assertEqual(call_args[1], "foo")

    def test_catalog_role_grant(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalog-roles",
                "grant",
                "--principal-role",
                "foo",
                "--catalog",
                "bar",
                "baz",
            ],
        )
        print(mock_client)
        call_args = mock_client.assign_catalog_role_to_principal_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")
        self.assertEqual(call_args[2].catalog_role.name, "baz")

    def test_catalog_role_revoke(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "catalog-roles",
                "revoke",
                "--principal-role",
                "foo",
                "--catalog",
                "bar",
                "baz",
            ],
        )
        print(mock_client)
        call_args = mock_client.revoke_catalog_role_from_principal_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")
        self.assertEqual(call_args[2], "baz")

    def test_catalog_roles_summarize(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.list_grants_for_catalog_role.return_value.grants = []
        mock_client.get_catalog_role.return_value = CatalogRole(
            name="foo",
            entity_version=1,
        )
        mock_client.list_assignee_principal_roles_for_catalog_role.return_value.roles = []
        self.mock_execute(
            mock_client, ["catalog-roles", "summarize", "foo", "--catalog", "bar"]
        )
        mock_client.get_catalog_role.assert_called_with("bar", "foo")
        mock_client.list_grants_for_catalog_role.assert_called_with("bar", "foo")
