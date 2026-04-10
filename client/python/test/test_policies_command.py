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

import unittest
from typing import cast
from unittest.mock import patch, MagicMock
from cli_test_utils import CLITestBase
from apache_polaris.cli.options.parser import Parser
from apache_polaris.cli.command import Command
from apache_polaris.cli.command.policies import PoliciesCommand


class TestPoliciesCommand(CLITestBase):
    def test_policy_attach_paremters(self) -> None:
        options = Parser.parse(
            [
                "policies",
                "attach",
                "policy-name",
                "--catalog",
                "cat",
                "--attachment-type",
                "catalog",
                "--parameters",
                "key=value",
            ]
        )
        command = Command.from_options(options)
        command = cast(PoliciesCommand, command)
        self.assertIsInstance(command.parameters, dict)
        self.assertEqual({"key": "value"}, command.parameters)

    @patch("apache_polaris.cli.command.policies.PolicyAPI")
    def test_policy_list(self, mock_policy_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_policy_api = mock_policy_api_class.return_value
        mock_policy_api.list_policies.return_value.to_json.return_value = "{}"
        self.mock_execute(
            mock_client,
            ["policies", "list", "--catalog", "my-catalog", "--namespace", "ns1"],
        )
        mock_policy_api.list_policies.assert_called_once_with(
            prefix="my-catalog", namespace="ns1", policy_type=None
        )
        mock_policy_api.get_applicable_policies.return_value.applicable_policies = []
        self.mock_execute(
            mock_client,
            [
                "policies",
                "list",
                "--catalog",
                "my-catalog",
                "--namespace",
                "ns1",
                "--applicable",
            ],
        )
        mock_policy_api.get_applicable_policies.assert_called_once_with(
            prefix="my-catalog", namespace="ns1", policy_type=None
        )

    @patch("apache_polaris.cli.command.policies.PolicyAPI")
    def test_policy_attach(self, mock_policy_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_policy_api = mock_policy_api_class.return_value
        self.mock_execute(
            mock_client,
            [
                "policies",
                "attach",
                "my-policy",
                "--catalog",
                "my-catalog",
                "--attachment-type",
                "catalog",
            ],
        )
        mock_policy_api.attach_policy.assert_called_once()
        _, kwargs = mock_policy_api.attach_policy.call_args
        self.assertEqual(kwargs["prefix"], "my-catalog")
        self.assertEqual(kwargs["policy_name"], "my-policy")

    @patch("apache_polaris.cli.command.policies.PolicyAPI")
    def test_policy_detach(self, mock_policy_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_policy_api = mock_policy_api_class.return_value
        self.mock_execute(
            mock_client,
            [
                "policies",
                "detach",
                "my-policy",
                "--catalog",
                "my-catalog",
                "--attachment-type",
                "catalog",
            ],
        )
        mock_policy_api.detach_policy.assert_called_once()
        _, kwargs = mock_policy_api.detach_policy.call_args
        self.assertEqual(kwargs["prefix"], "my-catalog")
        self.assertEqual(kwargs["policy_name"], "my-policy")

    @patch("apache_polaris.cli.command.policies.PolicyAPI")
    def test_policy_update(self, mock_policy_api_class: MagicMock) -> None:
        mock_client = self.build_mock_client()
        mock_policy_api = mock_policy_api_class.return_value
        mock_policy_api.load_policy.return_value.policy.version = 1

        with patch(
            "builtins.open",
            unittest.mock.mock_open(
                read_data='{"version": "2025-02-03", "enable": true, "config": {"target_file_size_bytes": 134217728, "compaction_strategy": "bin-pack", "max-concurrent-file-group-rewrites": 5, "key1": "value1"}}'
            ),
        ):
            self.mock_execute(
                mock_client,
                [
                    "policies",
                    "update",
                    "my-policy",
                    "--catalog",
                    "my-catalog",
                    "--policy-file",
                    "dummy.json",
                    "--policy-description",
                    "dummy policy",
                ],
            )
            mock_policy_api.update_policy.assert_called_once()
            _, kwargs = mock_policy_api.update_policy.call_args
            self.assertEqual(kwargs["prefix"], "my-catalog")
            self.assertEqual(kwargs["policy_name"], "my-policy")
            self.assertEqual(
                kwargs["update_policy_request"].description, "dummy policy"
            )
            self.assertEqual(kwargs["update_policy_request"].current_policy_version, 1)
