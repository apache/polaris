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


class TestProfilesCommand(CLITestBase):
    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.load_profiles",
        return_value={},
    )
    def test_profile_list(
        self, mock_load_profiles: MagicMock, mock_exists: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "list"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profiles:", output)

    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.load_profiles",
        return_value={
            "dev": {
                "client_id": "root",
                "client_secret": "s3cr3t",
                "host": "localhost",
                "port": 8181,
                "realm": "",
                "header": "Polaris-Realm",
            }
        },
    )
    def test_profile_get(
        self, mock_load_profiles: MagicMock, mock_exists: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "get", "dev"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profile dev:", output)
            self.assertIn("**cr3t", output)
            self.assertNotIn("s3cr3t", output)

    @patch("apache_polaris.cli.command.profiles.save_profiles")
    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.load_profiles",
        return_value={},
    )
    @patch("builtins.input")
    @patch("apache_polaris.cli.command.profiles.getpass")
    def test_profile_create(
        self,
        mock_getpass: MagicMock,
        mock_input: MagicMock,
        mock_load_profiles: MagicMock,
        mock_exists: MagicMock,
        mock_save_profiles: MagicMock,
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = False
        mock_getpass.return_value = "s3cr3t"
        mock_input.side_effect = [
            "root",
            "localhost",
            8181,
            "",
            "Polaris-Realm",
        ]
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "create", "dev"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profile dev created successfully.", output)
        mock_getpass.assert_called_once_with("Polaris Client Secret: ")
        mock_save_profiles.assert_called_once()

    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.load_profiles",
        return_value={"dev": {}},
    )
    @patch("apache_polaris.cli.command.profiles.sys.exit")
    def test_profile_create_existing_fails(
        self, mock_exit: MagicMock, mock_load_profiles: MagicMock, mock_exists: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO):
            self.mock_execute(mock_client, ["profiles", "create", "dev"])
        mock_exit.assert_called_once_with(1)

    @patch("apache_polaris.cli.command.profiles.save_profiles")
    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.load_profiles",
        return_value={
            "dev": {
                "client_id": "root",
                "client_secret": "s3cr3t",
                "host": "localhost",
                "port": 8181,
                "realm": "",
                "header": "Polaris-Realm",
            }
        },
    )
    def test_profile_delete(
        self,
        mock_load_profiles: MagicMock,
        mock_exists: MagicMock,
        mock_save_profiles: MagicMock,
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "delete", "dev"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profile dev deleted successfully.", output)
        mock_save_profiles.assert_called_once()

    @patch("apache_polaris.cli.command.profiles.save_profiles")
    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.load_profiles",
        return_value={
            "dev": {
                "client_id": "root",
                "client_secret": "s3cr3t",
                "host": "localhost",
                "port": 8181,
                "realm": "",
                "header": "Polaris-Realm",
            }
        },
    )
    @patch("builtins.input")
    @patch("apache_polaris.cli.command.profiles.getpass")
    def test_profile_update(
        self,
        mock_getpass: MagicMock,
        mock_input: MagicMock,
        mock_load_profiles: MagicMock,
        mock_exists: MagicMock,
        mock_save_profiles: MagicMock,
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        mock_getpass.return_value = "new-secret"
        mock_input.side_effect = [
            "new-id",
            "newhost",
            9090,
            "myrealm",
            "Polaris-Realm",
        ]
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "update", "dev"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profile dev updated successfully.", output)
        mock_getpass.assert_called_once()
        self.assertIn("Polaris Client Secret [**cr3t]: ", mock_getpass.call_args[0][0])
        mock_save_profiles.assert_called_once()
