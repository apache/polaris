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
from apache_polaris.sdk.management import PrincipalRole


class TestPrincipalRolesCommand(CLITestBase):
    def test_principal_role_create(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            ["principal-roles", "create", "foo"],
        )
        call_args = mock_client.create_principal_role.call_args[0]
        self.assertEqual(call_args[0].principal_role.name, "foo")

    def test_principal_role_get(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            ["principal-roles", "get", "foo"],
        )
        call_args = mock_client.get_principal_role.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_role_list(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            ["principal-roles", "list"],
        )
        mock_client.list_principal_roles.assert_called()

        self.mock_execute(
            mock_client,
            ["principal-roles", "list", "--principal", "foo"],
        )
        call_args = mock_client.list_principal_roles_assigned.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_role_delete(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            ["principal-roles", "delete", "foo"],
        )
        call_args = mock_client.delete_principal_role.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_role_update(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_principal_role.return_value = PrincipalRole(
            name="foo",
            entity_version=1,
        )
        self.mock_execute(
            mock_client,
            ["principal-roles", "update", "foo", "--set-property", "key=value"],
        )
        call_args = mock_client.update_principal_role.call_args[0][0]
        self.assertEqual(call_args, "foo")

        self.mock_execute(
            mock_client,
            ["principal-roles", "update", "foo", "--remove-property", "key"],
        )
        call_args = mock_client.update_principal_role.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_role_grant(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            ["principal-roles", "grant", "bar", "--principal", "foo"],
        )
        call_args = mock_client.assign_principal_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1].principal_role.name, "bar")

        self.mock_execute(
            mock_client,
            [
                "privileges",
                "catalog",
                "grant",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "TABLE_READ_DATA",
            ],
        )
        call_args = mock_client.add_grant_to_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")
        self.assertEqual(call_args[2].grant.privilege.value, "TABLE_READ_DATA")

    def test_principal_role_revoke(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            ["principal-roles", "revoke", "bar", "--principal", "foo"],
        )
        call_args = mock_client.revoke_principal_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")

    def test_principal_role_list_catalog_role(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.list_principal_roles.return_value.roles = []
        self.mock_execute(
            mock_client, ["principal-roles", "list", "--catalog-role", "bar"]
        )
        mock_client.list_principal_roles.assert_called_with("bar")

    def test_principal_role_summarize(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_principal_role.return_value = PrincipalRole(
            name="foo",
            entity_version=1,
        )
        mock_client.list_assignee_principals_for_principal_role.return_values.principals = []
        mock_client.list_catalogs.return_value.catalogs = []
        self.mock_execute(mock_client, ["principal-roles", "summarize", "foo"])
        mock_client.get_principal_role.assert_called_with("foo")
        mock_client.list_assignee_principals_for_principal_role.assert_called_with(
            "foo"
        )
