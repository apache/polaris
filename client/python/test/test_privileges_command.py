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


class TestPrivilegesCommand(CLITestBase):
    def test_privilege_commands_validation(self) -> None:
        mock_client = self.build_mock_client()
        # Invalid privilege
        self.check_exception(
            lambda: self.mock_execute(
                mock_client,
                [
                    "privileges",
                    "catalog",
                    "grant",
                    "--catalog",
                    "foo",
                    "--catalog-role",
                    "bar",
                    "TABLE_READ_MORE_BOOKS",
                ],
            ),
            "catalog privilege: TABLE_READ_MORE_BOOKS",
        )

    def test_privilege_grant(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "privileges",
                "namespace",
                "grant",
                "--namespace",
                "a.b.c",
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
        self.assertEqual(call_args[2].grant.namespace, ["a", "b", "c"])

        self.mock_execute(
            mock_client,
            [
                "privileges",
                "table",
                "grant",
                "--namespace",
                "a.b.c",
                "--table",
                "t",
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
        self.assertEqual(call_args[2].grant.namespace, ["a", "b", "c"])
        self.assertEqual(call_args[2].grant.table_name, "t")

        self.mock_execute(
            mock_client,
            [
                "privileges",
                "view",
                "grant",
                "--namespace",
                "a.b.c",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "--view",
                "v",
                "VIEW_FULL_METADATA",
            ],
        )
        call_args = mock_client.add_grant_to_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")
        self.assertEqual(call_args[2].grant.privilege.value, "VIEW_FULL_METADATA")
        self.assertEqual(call_args[2].grant.namespace, ["a", "b", "c"])
        self.assertEqual(call_args[2].grant.view_name, "v")

    def test_privilege_revoke(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(
            mock_client,
            [
                "privileges",
                "catalog",
                "revoke",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "TABLE_READ_DATA",
            ],
        )
        call_args = mock_client.revoke_grant_from_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")
        self.assertEqual(call_args[2], False)
        self.assertEqual(call_args[3].grant.privilege.value, "TABLE_READ_DATA")

        self.mock_execute(
            mock_client,
            [
                "privileges",
                "table",
                "revoke",
                "--namespace",
                "a.b.c",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "--table",
                "t",
                "--cascade",
                "TABLE_READ_DATA",
            ],
        )
        call_args = mock_client.revoke_grant_from_catalog_role.call_args[0]
        self.assertEqual(call_args[0], "foo")
        self.assertEqual(call_args[1], "bar")
        self.assertEqual(call_args[2], True)
        self.assertEqual(call_args[3].grant.privilege.value, "TABLE_READ_DATA")
        self.assertEqual(call_args[3].grant.namespace, ["a", "b", "c"])
        self.assertEqual(call_args[3].grant.table_name, "t")
