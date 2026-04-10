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
from unittest.mock import patch, MagicMock, mock_open, ANY
from cli_test_utils import CLITestBase


class TestProfilesCommand(CLITestBase):
    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.open",
        new_callable=mock_open,
        read_data="{}",
    )
    def test_profile_list(self, mock_file: MagicMock, mock_exists: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "list"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profiles:", output)

    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.open",
        new_callable=mock_open,
        read_data='{"dev": {"client_id": "root", "client_secret": "s3cr3t", "host": "localhost", "port": 8181, "realm": "", "header": "Polaris-Realm"}}',
    )
    def test_profile_get(self, mock_file: MagicMock, mock_exists: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "get", "dev"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profile dev:", output)

    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.open",
        new_callable=mock_open,
        read_data="{}",
    )
    @patch("builtins.input")
    @patch("apache_polaris.cli.command.profiles.os.makedirs")
    def test_profile_create(
        self,
        mock_makedirs: MagicMock,
        mock_input: MagicMock,
        mock_file: MagicMock,
        mock_exists: MagicMock,
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = False
        mock_input.side_effect = [
            "root",
            "s3cr3t",
            "localhost",
            8181,
            "",
            "Polaris-Realm",
        ]
        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            self.mock_execute(mock_client, ["profiles", "create", "dev"])
            output = mock_stdout.getvalue()
            self.assertIn("Polaris profile dev created successfully.", output)
        mock_file.assert_called_with(ANY, "w")

    @patch("apache_polaris.cli.command.profiles.os.path.exists")
    @patch(
        "apache_polaris.cli.command.profiles.open",
        new_callable=mock_open,
        read_data='{"dev": {}}',
    )
    @patch("apache_polaris.cli.command.profiles.sys.exit")
    def test_profile_create_existing_fails(
        self, mock_exit: MagicMock, mock_file: MagicMock, mock_exists: MagicMock
    ) -> None:
        mock_client = self.build_mock_client()
        mock_exists.return_value = True
        with patch("sys.stdout", new_callable=io.StringIO):
            self.mock_execute(mock_client, ["profiles", "create", "dev"])
        mock_exit.assert_called_once_with(1)
