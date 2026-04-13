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
from apache_polaris.sdk.management import (
    Principal,
    PrincipalWithCredentials,
    PrincipalWithCredentialsCredentials,
)
from pydantic import SecretStr


class TestPrincipalsCommand(CLITestBase):
    def test_principal_create(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.create_principal.return_value = PrincipalWithCredentials(
            principal=Principal(
                name="foo", type="SERVICE", properties={"key": "value"}
            ),
            credentials=PrincipalWithCredentialsCredentials(
                client_id="test-client-id",
                client_secret=SecretStr("test-client-secret"),
            ),
        )
        self.mock_execute(
            mock_client, ["principals", "create", "foo", "--property", "key=value"]
        )
        call_args = mock_client.create_principal.call_args[0][0]
        self.assertEqual(call_args.principal.name, "foo")
        self.assertEqual(call_args.principal.client_id, None)
        self.assertEqual(call_args.principal.properties, {"key": "value"})

    def test_principal_list(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(mock_client, ["principals", "list"])
        mock_client.list_principals.assert_called_once()

    def test_principal_get(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(mock_client, ["principals", "get", "foo"])
        call_args = mock_client.get_principal.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_delete(self) -> None:
        mock_client = self.build_mock_client()
        self.mock_execute(mock_client, ["principals", "delete", "foo"])
        call_args = mock_client.delete_principal.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_update(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_principal.return_value = Principal(
            name="foo", type="SERVICE", properties={"key": "value"}, entity_version=1
        )
        self.mock_execute(
            mock_client, ["principals", "update", "foo", "--set-property", "key=value"]
        )
        call_args = mock_client.update_principal.call_args[0][0]
        self.assertEqual(call_args, "foo")

        self.mock_execute(
            mock_client, ["principals", "update", "foo", "--remove-property", "key"]
        )
        call_args = mock_client.update_principal.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_rotate_credentials(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.rotate_credentials.return_value = PrincipalWithCredentials(
            principal=Principal(
                name="foo", type="SERVICE", properties={"key": "value"}
            ),
            credentials=PrincipalWithCredentialsCredentials(
                client_id="test-client-id",
                client_secret=SecretStr("test-client-secret"),
            ),
        )
        self.mock_execute(mock_client, ["principals", "rotate-credentials", "foo"])
        call_args = mock_client.rotate_credentials.call_args[0][0]
        self.assertEqual(call_args, "foo")

    def test_principal_reset(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.reset_credentials.return_value = PrincipalWithCredentials(
            principal=Principal(
                name="foo", type="SERVICE", properties={"key": "value"}
            ),
            credentials=PrincipalWithCredentialsCredentials(
                client_id="test-client-id",
                client_secret=SecretStr("test-client-secret"),
            ),
        )
        self.mock_execute(
            mock_client,
            [
                "principals",
                "reset",
                "test",
                "--new-client-id",
                "e469c048cf866df1",
                "--new-client-secret",
                "e469c048cf866dfae469c048cf866df1",
            ],
        )
        call_args = mock_client.reset_credentials.call_args[0]
        self.assertEqual(call_args[0], "test")
        self.assertEqual(call_args[1].client_id, "e469c048cf866df1")
        self.assertEqual(call_args[1].client_secret, "e469c048cf866dfae469c048cf866df1")

        self.mock_execute(mock_client, ["principals", "reset", "test"])
        call_args = mock_client.reset_credentials.call_args[0]
        self.assertEqual(call_args[0], "test")
        self.assertEqual(call_args[1], None)

        self.mock_execute(
            mock_client,
            ["principals", "reset", "test", "--new-client-id", "e469c048cf866df1"],
        )
        call_args = mock_client.reset_credentials.call_args[0]
        self.assertEqual(call_args[0], "test")
        self.assertEqual(call_args[1].client_id, "e469c048cf866df1")
        self.assertEqual(call_args[1].client_secret, None)

        self.mock_execute(
            mock_client,
            [
                "principals",
                "reset",
                "test",
                "--new-client-secret",
                "e469c048cf866dfae469c048cf866df1",
            ],
        )
        call_args = mock_client.reset_credentials.call_args[0]
        self.assertEqual(call_args[0], "test")
        self.assertEqual(call_args[1].client_id, None)
        self.assertEqual(call_args[1].client_secret, "e469c048cf866dfae469c048cf866df1")

    def test_principal_access(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_principal.return_value = Principal(
            name="foo", type="SERVICE", properties={"key": "value"}, entity_version=1
        )
        mock_client.list_principal_roles_assigned.return_values.roles = []
        mock_client.list_catalogs.return_values.catalogs = []
        self.mock_execute(mock_client, ["principals", "access", "foo"])
        mock_client.get_principal.assert_called_with("foo")
        mock_client.list_principal_roles_assigned.assert_called_with("foo")

    def test_principal_summarize(self) -> None:
        mock_client = self.build_mock_client()
        mock_client.get_principal.return_value = Principal(
            name="foo", type="SERVICE", properties={"key": "value"}, entity_version=1
        )
        mock_client.list_principal_roles_assigned.return_values.roles = []
        mock_client.list_catalogs.return_values.catalogs = []
        self.mock_execute(mock_client, ["principals", "summarize", "foo"])
        mock_client.get_principal.assert_called_with("foo")
        mock_client.list_principal_roles_assigned.assert_called_with("foo")
