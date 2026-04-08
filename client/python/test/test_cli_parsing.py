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
import unittest
from functools import reduce
from typing import List, Any, Callable, Dict, cast
from unittest.mock import patch, MagicMock

from apache_polaris.cli.command import Command
from apache_polaris.cli.command.policies import PoliciesCommand
from apache_polaris.cli.options.parser import Parser
from apache_polaris.sdk.management import PolarisDefaultApi

INVALID_ARGS = 2


class TestCliParsing(unittest.TestCase):
    def test_invalid_commands(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["not-real-command!", "list"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["catalogs", "not-real-subcommand"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["catalogs", "create"])  # missing required input
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(
                ["catalogs", "create", "catalog_name", "--type", "BANANA"]
            )  # invalid catalog type
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(
                ["catalogs", "create", "catalog_name", "--set-property", "foo=bar"]
            )  # can't use --set-property on create
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["catalogs", "get", "catalog_name", "--fake-flag"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(
                ["catalogs", "update", "catalog_name", "--property", "foo=bar"]
            )
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(
                [
                    "catalogs",
                    "create",
                    "catalog_name",
                    "--type",
                    "EXTERNAL",
                    "--remote-url",
                    "gone",
                ]
            )  # remote-url deprecated
            self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["principals", "create", "name", "--type", "bad"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["principals", "update", "name", "--client-id", "something"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["find"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["find", "my_table", "--type", "invalid-type"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["table"])  # missing subcommand
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(Exception) as cm_exc:
            options = Parser.parse(
                ["find", " ", "--catalog", "my_catalog"]
            )  # empty identifier
            Command.from_options(options)
        self.assertIn("The search identifier cannot be empty", str(cm_exc.exception))

        with self.assertRaises(Exception) as cm_missing:
            options = Parser.parse(["tables", "list"])  # missing catalog/namespace
            Command.from_options(options)
        self.assertIn("Missing required argument", str(cm_missing.exception))

        with self.assertRaises(Exception) as cm_exc:
            options = Parser.parse(
                ["tables", "get", " ", "--catalog", "my_catalog", "--namespace", "ns"]
            )  # empty table name
            Command.from_options(options)
        self.assertIn("The table name cannot be empty", str(cm_exc.exception))

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(
                [
                    "privileges",
                    "catalog",
                    "--catalog",
                    "c",
                    "--catalog-role",
                    "r",
                    "privilege",
                    "grant",
                ]
            )
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(
                [
                    "privileges",
                    "--catalog",
                    "c",
                    "--catalog-role",
                    "r",
                    "catalog",
                    "grant",
                    "privilege",
                    "--namespace",
                    "unexpected!",
                ]
            )
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["namespaces"])  # missing subcommand
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["namespaces", "create"])  # missing required namespace input
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["namespaces", "delete"])  # missing required namespace input
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["namespaces", "not-real-subcommand"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["namespaces", "get", "ns", "--fake-flag"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["profiles"])  # missing subcommand
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["profiles", "create"])  # missing required profile name
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["profiles", "not-real-subcommand"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["setup"])  # missing subcommand
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["setup", "apply"])  # missing required setup config
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["setup", "not-real-subcommand"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["policies"])  # missing subcommand
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["policies", "create"])  # missing required policy name
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(["policies", "not-real-subcommand"])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(Exception) as cm_exc:
            options = Parser.parse(["namespaces", "list"])  # missing catalog
            Command.from_options(options)
        self.assertIn("Missing required argument", str(cm_exc.exception))

        with self.assertRaises(Exception) as cm_exc:
            options = Parser.parse(
                ["namespaces", "create", "my_ns"]
            )  # missing catalog
            Command.from_options(options)
        self.assertIn("Missing required argument", str(cm_exc.exception))

    def _check_usage_output(self, f: Callable[[], Any], needle: str = "usage:") -> None:
        with (
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
            patch("sys.stderr", new_callable=io.StringIO),
        ):
            with self.assertRaises(SystemExit) as cm:
                f()
            self.assertEqual(cm.exception.code, 0)
            help_output = str(mock_stdout.getvalue())
            self.assertIn("usage:", help_output)
        print(help_output)

    def test_usage(self) -> None:
        self._check_usage_output(lambda: Parser.parse(["--help"]))
        self._check_usage_output(lambda: Parser.parse(["catalogs", "--help"]))
        self._check_usage_output(lambda: Parser.parse(["catalogs", "create", "--help"]))
        self._check_usage_output(
            lambda: Parser.parse(["catalogs", "create", "something", "--help"])
        )
        self._check_usage_output(lambda: Parser.parse(["namespaces", "--help"]))
        self._check_usage_output(
            lambda: Parser.parse(["namespaces", "create", "--help"])
        )
        self._check_usage_output(lambda: Parser.parse(["profiles", "--help"]))
        self._check_usage_output(lambda: Parser.parse(["setup", "--help"]))
        self._check_usage_output(lambda: Parser.parse(["policies", "--help"]))
        self._check_usage_output(lambda: Parser.parse(["tables", "--help"]))
        self._check_usage_output(lambda: Parser.parse(["find", "--help"]))

    def test_extended_usage(self) -> None:
        self._check_usage_output(
            lambda: Parser._build_parser().parse_args(["--help"], "input:")
        )
        self._check_usage_output(
            lambda: Parser._build_parser().parse_args(["catalogs", "--help"], "input:")
        )
        self._check_usage_output(
            lambda: Parser._build_parser().parse_args(
                ["catalogs", "create", "--help"], "input:"
            )
        )
        self._check_usage_output(
            lambda: Parser._build_parser().parse_args(
                ["catalogs", "create", "c", "--help"], "input:"
            )
        )
        self._check_usage_output(
            lambda: Parser._build_parser().parse_args(
                ["privileges", "table", "grant", "--help"], "input:"
            )
        )
        self._check_usage_output(
            lambda: Parser.parse(["catalogs", "create", "something", "--help"]),
            "input:",
        )

    def test_parsing_valid_commands(self) -> None:
        Parser.parse(["catalogs", "create", "catalog_name"])
        Parser.parse(["catalogs", "create", "catalog_name", "--type", "internal"])
        Parser.parse(["catalogs", "create", "catalog_name", "--type", "INTERNAL"])
        Parser.parse(["catalogs", "list"])
        Parser.parse(["catalogs", "get", "catalog_name"])
        Parser.parse(["catalogs", "summarize", "catalog_name"])
        Parser.parse(["principals", "list"])
        Parser.parse(["--host", "some-host", "catalogs", "list"])
        Parser.parse(
            ["--base-url", "https://customservice.com/subpath", "catalogs", "list"]
        )
        Parser.parse(["--proxy", "http://proxy:8080", "catalogs", "list"])
        Parser.parse(["--debug", "catalogs", "list"])
        Parser.parse(["--port", "8182", "catalogs", "list"])
        Parser.parse(
            ["--client-id", "cid", "--client-secret", "csecret", "catalogs", "list"]
        )
        Parser.parse(["--access-token", "token", "catalogs", "list"])
        Parser.parse(
            ["--realm", "my_realm", "--header", "X-Custom", "catalogs", "list"]
        )
        Parser.parse(["--profile", "dev", "catalogs", "list"])
        Parser.parse(
            [
                "privileges",
                "catalog",
                "grant",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "TABLE_READ_DATA",
            ]
        )
        Parser.parse(
            [
                "privileges",
                "table",
                "grant",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "--namespace",
                "n",
                "--table",
                "t",
                "TABLE_READ_DATA",
            ]
        )
        Parser.parse(
            [
                "privileges",
                "table",
                "revoke",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
                "--namespace",
                "n",
                "--table",
                "t",
                "TABLE_READ_DATA",
            ]
        )
        Parser.parse(
            [
                "privileges",
                "list",
                "--catalog",
                "foo",
                "--catalog-role",
                "bar",
            ]
        )
        Parser.parse(["namespaces", "list", "--catalog", "my_catalog"])
        Parser.parse(["namespaces", "create", "my_ns", "--catalog", "my_catalog"])
        Parser.parse(["namespaces", "delete", "my_ns", "--catalog", "my_catalog"])
        Parser.parse(["namespaces", "get", "my_ns", "--catalog", "my_catalog"])
        Parser.parse(["namespaces", "summarize", "my_ns", "--catalog", "my_catalog"])
        Parser.parse(["profiles", "create", "dev"])
        Parser.parse(["profiles", "delete", "dev"])
        Parser.parse(["profiles", "update", "dev"])
        Parser.parse(["profiles", "get", "dev"])
        Parser.parse(["profiles", "list"])
        Parser.parse(["setup", "apply", "config.yaml"])
        Parser.parse(["setup", "export"])
        Parser.parse(
            ["policies", "list", "--catalog", "c", "--namespace", "ns"]
        )
        Parser.parse(
            ["policies", "get", "my_policy", "--catalog", "c", "--namespace", "ns"]
        )

    # These commands are valid for parsing, but may cause errors within the command itself
    def test_parse_argparse_valid_commands(self) -> None:
        Parser.parse(["catalogs", "create", "catalog_name", "--type", "internal"])
        Parser.parse(
            [
                "privileges",
                "table",
                "grant",
                "--namespace",
                "n",
                "--table",
                "t",
                "TABLE_READ_DATA",
            ]
        )
        Parser.parse(
            [
                "privileges",
                "catalog",
                "grant",
                "--catalog",
                "c",
                "--catalog-role",
                "r",
                "fake-privilege",
            ]
        )
        Parser.parse(["find", "my_table"])
        Parser.parse(["find", "my_table", "--catalog", "my_catalog"])
        Parser.parse(["find", "my_table", "--catalog", "my_catalog", "--type", "table"])
        Parser.parse(
            ["tables", "list", "--catalog", "my_catalog", "--namespace", "ns1.ns2"]
        )
        Parser.parse(
            [
                "tables",
                "get",
                "my_table",
                "--catalog",
                "my_catalog",
                "--namespace",
                "ns1.ns2",
            ]
        )
        Parser.parse(
            [
                "tables",
                "summarize",
                "my_table",
                "--catalog",
                "my_catalog",
                "--namespace",
                "ns1.ns2",
            ]
        )
        Parser.parse(
            [
                "tables",
                "delete",
                "my_table",
                "--catalog",
                "my_catalog",
                "--namespace",
                "ns1.ns2",
            ]
        )
        Parser.parse(
            [
                "tables",
                "summarize",
                "my_table",
                "--catalog",
                "my_catalog",
                "--namespace",
                "ns1.ns2",
            ]
        )
        Parser.parse(
            [
                "namespaces",
                "create",
                "inner_ns",
                "--catalog",
                "my_catalog",
                "--location",
                "s3://bucket/path",
                "--property",
                "key=value",
            ]
        )
        Parser.parse(
            [
                "namespaces",
                "list",
                "--catalog",
                "my_catalog",
                "--parent",
                "parent_ns",
            ]
        )
        Parser.parse(["setup", "apply", "config.yaml", "--dry-run"])
        Parser.parse(
            [
                "policies",
                "create",
                "my_policy",
                "--catalog",
                "c",
                "--namespace",
                "ns",
                "--policy-file",
                "policy.json",
                "--policy-type",
                "system.data-compaction",
                "--policy-description",
                "my description",
            ]
        )
        Parser.parse(
            [
                "policies",
                "delete",
                "my_policy",
                "--catalog",
                "c",
                "--namespace",
                "ns",
                "--detach-all",
            ]
        )
        Parser.parse(
            [
                "policies",
                "list",
                "--catalog",
                "c",
                "--namespace",
                "ns",
                "--applicable",
                "--target-name",
                "my_table",
                "--policy-type",
                "system.data-compaction",
            ]
        )
        Parser.parse(
            [
                "policies",
                "update",
                "my_policy",
                "--catalog",
                "c",
                "--namespace",
                "ns",
                "--policy-file",
                "policy.json",
                "--policy-description",
                "updated desc",
            ]
        )
        Parser.parse(
            [
                "policies",
                "attach",
                "my_policy",
                "--catalog",
                "c",
                "--namespace",
                "ns",
                "--attachment-type",
                "table-like",
                "--attachment-path",
                "ns.t",
                "--parameters",
                "key=value",
            ]
        )
        Parser.parse(
            [
                "policies",
                "detach",
                "my_policy",
                "--catalog",
                "c",
                "--namespace",
                "ns",
                "--attachment-type",
                "catalog",
            ]
        )
        Parser.parse(
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "azure",
                "--default-base-location",
                "abfss://container@account.dfs.core.windows.net",
                "--tenant-id",
                "tid",
            ]
        )
        Parser.parse(
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "s3",
                "--default-base-location",
                "s3://bucket/path",
                "--role-arn",
                "ra",
                "--endpoint",
                "http://s3.local:9000",
                "--sts-endpoint",
                "http://sts.local:9000",
                "--no-sts",
                "--no-kms",
                "--path-style-access",
                "--current-kms-key",
                "arn:aws:kms:key",
                "--allowed-kms-key",
                "k1",
                "--allowed-kms-key",
                "k2",
            ]
        )
        Parser.parse(
            [
                "catalogs",
                "create",
                "my-catalog",
                "--storage-type",
                "file",
                "--default-base-location",
                "/tmp/warehouse",
            ]
        )
        Parser.parse(
            [
                "catalogs",
                "create",
                "my-catalog",
                "--type",
                "external",
                "--storage-type",
                "s3",
                "--default-base-location",
                "s3://b/p",
                "--role-arn",
                "ra",
                "--catalog-connection-type",
                "iceberg-rest",
                "--catalog-uri",
                "u",
                "--catalog-authentication-type",
                "implicit",
                "--catalog-service-identity-type",
                "aws_iam",
                "--catalog-service-identity-iam-arn",
                "arn:aws:iam::12345:role/polaris",
            ]
        )

    def test_commands(self) -> None:
        def build_mock_client() -> MagicMock:
            client = MagicMock(spec=PolarisDefaultApi)
            client.call_tracker = dict()

            def capture_method(method_name: str) -> Callable[..., None]:
                def _capture(*args: Any, **kwargs: Any) -> None:
                    client.call_tracker["_method"] = method_name
                    for i, arg in enumerate(args):
                        client.call_tracker[i] = arg

                return _capture

            for method_name in dir(client):
                if callable(
                    getattr(client, method_name)
                ) and not method_name.startswith("__"):
                    setattr(
                        client,
                        method_name,
                        MagicMock(
                            name=method_name, side_effect=capture_method(method_name)
                        ),
                    )
            return client

        mock_client = build_mock_client()

        def mock_execute(input: List[str]) -> Dict[Any, Any]:
            mock_client.call_tracker = dict()

            # Assuming Parser and Command are used to parse input and generate commands
            options = Parser.parse(input)
            command = Command.from_options(options)

            try:
                command.execute(mock_client)
            except AttributeError as e:
                # Some commands may fail due to the mock, but the results should still match expectations
                print(f"Suppressed error: {e}")
            return mock_client.call_tracker

        def check_exception(f: Callable[[], Any], exception_str: str) -> None:
            throws = True
            try:
                f()
                throws = False
            except Exception as e:
                self.assertIn(exception_str, str(e))
            self.assertTrue(throws, "Exception should be raised")

        def check_arguments(
            result: Dict[Any, Any], method_name: str, args: Dict[Any, Any] = {}
        ) -> None:
            self.assertEqual(method_name, result["_method"])

            def get(obj: Any, arg_string: str) -> Any:
                attributes = arg_string.split(".")
                return reduce(getattr, attributes, obj)

            for arg, value in args.items():
                index, path = arg
                if path is not None:
                    self.assertEqual(value, get(result[index], path))
                else:
                    self.assertEqual(value, result[index])

        # Test various failing commands:
        check_exception(
            lambda: mock_execute(["catalogs", "create", "my-catalog"]), "--storage-type"
        )
        check_exception(
            lambda: mock_execute(
                ["catalogs", "create", "my-catalog", "--storage-type", "gcs"]
            ),
            "--default-base-location",
        )
        check_exception(
            lambda: mock_execute(["catalog-roles", "get", "foo"]), "--catalog"
        )
        check_exception(
            lambda: mock_execute(
                ["catalogs", "update", "foo", "--set-property", "bad-format"]
            ),
            "bad-format",
        )
        check_exception(
            lambda: mock_execute(
                [
                    "privileges",
                    "catalog",
                    "grant",
                    "--catalog",
                    "foo",
                    "--catalog-role",
                    "bar",
                    "TABLE_READ_MORE_BOOKS",
                ]
            ),
            "catalog privilege: TABLE_READ_MORE_BOOKS",
        )
        check_exception(
            lambda: mock_execute(
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
                ]
            ),
            "gcs",
        )
        check_exception(
            lambda: mock_execute(
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
                ]
            ),
            "--hive-warehouse",
        )
        check_exception(
            lambda: mock_execute(["namespaces", "list"]),
            "--catalog",
        )
        check_exception(
            lambda: mock_execute(["namespaces", "create", "my_ns"]),
            "--catalog",
        )
        check_exception(
            lambda: mock_execute(["namespaces", "delete", "my_ns"]),
            "--catalog",
        )
        check_exception(
            lambda: mock_execute(["policies", "create", "my_policy"]),
            "--catalog",
        )
        check_exception(
            lambda: mock_execute(
                [
                    "policies",
                    "create",
                    "my_policy",
                    "--catalog",
                    "c",
                    "--namespace",
                    "ns",
                ]
            ),
            "--policy-file",
        )
        check_exception(
            lambda: mock_execute(
                ["policies", "attach", "my_policy", "--catalog", "c"]
            ),
            "--attachment-type",
        )
        check_exception(
            lambda: mock_execute(["policies", "list", "--catalog", "c"]),
            "--namespace",
        )
        check_exception(
            lambda: mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "azure",
                    "--default-base-location",
                    "x",
                ]
            ),
            "azure",
        )
        check_exception(
            lambda: mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "s3",
                    "--default-base-location",
                    "x",
                    "--role-arn",
                    "ra",
                    "--tenant-id",
                    "tid",
                ]
            ),
            "s3",
        )
        check_exception(
            lambda: mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--type",
                    "external",
                    "--storage-type",
                    "file",
                    "--default-base-location",
                    "x",
                    "--catalog-service-identity-type",
                    "aws_iam",
                ]
            ),
            "--catalog-service-identity-iam-arn",
        )

        # Test various correct commands:
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "gcs",
                    "--default-base-location",
                    "x",
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "GCS",
                (0, "catalog.properties.default_base_location"): "x",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "S3",
                (0, "catalog.properties.default_base_location"): "x",
                (0, "catalog.storage_config_info.allowed_locations"): ["a", "b"],
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "S3",
                (0, "catalog.properties.default_base_location"): "x",
                (0, "catalog.storage_config_info.allowed_locations"): ["a"],
                (0, "catalog.storage_config_info.region"): "us-west-2",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "GCS",
                (0, "catalog.properties.default_base_location"): "x",
                (0, "catalog.storage_config_info.allowed_locations"): ["a", "b"],
                (0, "catalog.storage_config_info.gcs_service_account"): "sa",
            },
        )
        check_arguments(mock_execute(["catalogs", "list"]), "list_catalogs")
        check_arguments(
            mock_execute(
                ["--base-url", "https://customservice.com/subpath", "catalogs", "list"]
            ),
            "list_catalogs",
        )
        check_arguments(
            mock_execute(["catalogs", "delete", "foo"]),
            "delete_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalogs", "get", "foo"]),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalogs", "update", "foo", "--default-base-location", "x"]),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalogs", "update", "foo", "--set-property", "key=value"]),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                ["catalogs", "update", "foo", "--set-property", "listkey=k1=v1,k2=v2"]
            ),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalogs", "update", "foo", "--remove-property", "key"]),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "update",
                    "foo",
                    "--set-property",
                    "key=value",
                    "--default-base-location",
                    "x",
                ]
            ),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "get_catalog",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principals", "create", "foo", "--property", "key=value"]),
            "create_principal",
            {
                (0, "principal.name"): "foo",
                (0, "principal.client_id"): None,
                (0, "principal.properties"): {"key": "value"},
            },
        )
        check_arguments(
            mock_execute(["principals", "delete", "foo"]),
            "delete_principal",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principals", "get", "foo"]),
            "get_principal",
            {
                (0, None): "foo",
            },
        )
        check_arguments(mock_execute(["principals", "list"]), "list_principals")
        check_arguments(
            mock_execute(["principals", "rotate-credentials", "foo"]),
            "rotate_credentials",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                ["principals", "update", "foo", "--set-property", "key=value"]
            ),
            "get_principal",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principals", "update", "foo", "--remove-property", "key"]),
            "get_principal",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "create", "foo"]),
            "create_principal_role",
            {
                (0, "principal_role.name"): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "delete", "foo"]),
            "delete_principal_role",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "delete", "foo"]),
            "delete_principal_role",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "get", "foo"]),
            "get_principal_role",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "get", "foo"]),
            "get_principal_role",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "list"]), "list_principal_roles"
        )
        check_arguments(
            mock_execute(["principal-roles", "list", "--principal", "foo"]),
            "list_principal_roles_assigned",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                ["principal-roles", "update", "foo", "--set-property", "key=value"]
            ),
            "get_principal_role",
            {(0, None): "foo"},
        )
        check_arguments(
            mock_execute(
                ["principal-roles", "update", "foo", "--remove-property", "key"]
            ),
            "get_principal_role",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "grant", "bar", "--principal", "foo"]),
            "assign_principal_role",
            {
                (0, None): "foo",
                (1, "principal_role.name"): "bar",
            },
        )
        check_arguments(
            mock_execute(["principal-roles", "revoke", "bar", "--principal", "foo"]),
            "revoke_principal_role",
            {
                (0, None): "foo",
                (1, None): "bar",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalog-roles",
                    "create",
                    "foo",
                    "--catalog",
                    "bar",
                    "--property",
                    "key=value",
                ]
            ),
            "create_catalog_role",
            {
                (0, None): "bar",
                (1, "catalog_role.name"): "foo",
                (1, "catalog_role.properties"): {"key": "value"},
            },
        )
        check_arguments(
            mock_execute(["catalog-roles", "delete", "foo", "--catalog", "bar"]),
            "delete_catalog_role",
            {
                (0, None): "bar",
                (1, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalog-roles", "get", "foo", "--catalog", "bar"]),
            "get_catalog_role",
            {
                (0, None): "bar",
                (1, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalog-roles", "list", "foo"]),
            "list_catalog_roles",
            {
                (0, None): "foo",
            },
        )
        check_arguments(
            mock_execute(["catalog-roles", "list", "foo", "--principal-role", "bar"]),
            "list_catalog_roles_for_principal_role",
            {
                (0, None): "bar",
                (1, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalog-roles",
                    "update",
                    "foo",
                    "--catalog",
                    "bar",
                    "--set-property",
                    "key=value",
                ]
            ),
            "get_catalog_role",
            {
                (0, None): "bar",
                (1, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalog-roles",
                    "update",
                    "foo",
                    "--catalog",
                    "bar",
                    "--remove-property",
                    "key",
                ]
            ),
            "get_catalog_role",
            {
                (0, None): "bar",
                (1, None): "foo",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalog-roles",
                    "grant",
                    "--principal-role",
                    "foo",
                    "--catalog",
                    "bar",
                    "baz",
                ]
            ),
            "assign_catalog_role_to_principal_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, "catalog_role.name"): "baz",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalog-roles",
                    "revoke",
                    "--principal-role",
                    "foo",
                    "--catalog",
                    "bar",
                    "baz",
                ]
            ),
            "revoke_catalog_role_from_principal_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, None): "baz",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "privileges",
                    "catalog",
                    "grant",
                    "--catalog",
                    "foo",
                    "--catalog-role",
                    "bar",
                    "TABLE_READ_DATA",
                ]
            ),
            "add_grant_to_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, "grant.privilege.value"): "TABLE_READ_DATA",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "privileges",
                    "catalog",
                    "revoke",
                    "--catalog",
                    "foo",
                    "--catalog-role",
                    "bar",
                    "TABLE_READ_DATA",
                ]
            ),
            "revoke_grant_from_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, None): False,
                (3, "grant.privilege.value"): "TABLE_READ_DATA",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "add_grant_to_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, "grant.privilege.value"): "TABLE_READ_DATA",
                (2, "grant.namespace"): ["a", "b", "c"],
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "add_grant_to_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, "grant.privilege.value"): "TABLE_READ_DATA",
                (2, "grant.namespace"): ["a", "b", "c"],
                (2, "grant.table_name"): "t",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "revoke_grant_from_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, None): True,
                (3, "grant.privilege.value"): "TABLE_READ_DATA",
                (3, "grant.namespace"): ["a", "b", "c"],
                (3, "grant.table_name"): "t",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "add_grant_to_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, "grant.privilege.value"): "VIEW_FULL_METADATA",
                (2, "grant.namespace"): ["a", "b", "c"],
                (2, "grant.view_name"): "v",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "ICEBERG_REST",
                (0, "catalog.connection_config_info.remote_catalog_name"): "i",
                (0, "catalog.connection_config_info.uri"): "u",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "ICEBERG_REST",
                (0, "catalog.connection_config_info.remote_catalog_name"): "c",
                (0, "catalog.connection_config_info.uri"): "u",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.authentication_type",
                ): "OAUTH",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.token_uri",
                ): "u",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.client_id",
                ): "i",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.scopes",
                ): ["s1", "s2"],
                (0, "catalog.storage_config_info.storage_type"): "GCS",
                (0, "catalog.properties.default_base_location"): "dbl",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "ICEBERG_REST",
                (0, "catalog.connection_config_info.remote_catalog_name"): "i",
                (0, "catalog.connection_config_info.uri"): "u",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.role_arn",
                ): "a",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.signing_region",
                ): "s",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "ICEBERG_REST",
                (0, "catalog.connection_config_info.remote_catalog_name"): "i",
                (0, "catalog.connection_config_info.uri"): "u",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.role_arn",
                ): "a",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.signing_region",
                ): "s",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.role_session_name",
                ): "n",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.external_id",
                ): "i",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.signing_name",
                ): "g",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "HADOOP",
                (0, "catalog.connection_config_info.warehouse"): "h",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.authentication_type",
                ): "IMPLICIT",
                (0, "catalog.connection_config_info.uri"): "u",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "HIVE",
                (0, "catalog.connection_config_info.warehouse"): "h",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.authentication_type",
                ): "IMPLICIT",
                (0, "catalog.connection_config_info.uri"): "u",
            },
        )
        check_arguments(
            mock_execute(
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
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (0, "catalog.connection_config_info.connection_type"): "HIVE",
                (0, "catalog.connection_config_info.warehouse"): "/warehouse/path",
                (
                    0,
                    "catalog.connection_config_info.authentication_parameters.authentication_type",
                ): "OAUTH",
                (
                    0,
                    "catalog.connection_config_info.uri",
                ): "thrift://hive-metastore:9083",
            },
        )

        check_arguments(
            mock_execute(
                [
                    "principals",
                    "reset",
                    "test",
                    "--new-client-id",
                    "e469c048cf866df1",
                    "--new-client-secret",
                    "e469c048cf866dfae469c048cf866df1",
                ]
            ),
            "reset_credentials",
            {
                (0, None): "test",
                (1, "client_id"): "e469c048cf866df1",
                (1, "client_secret"): "e469c048cf866dfae469c048cf866df1",
            },
        )

        check_arguments(
            mock_execute(["principals", "reset", "test"]),
            "reset_credentials",
            {
                (0, None): "test",
                (1, None): None,
            },
        )

        check_arguments(
            mock_execute(
                ["principals", "reset", "test", "--new-client-id", "e469c048cf866df1"]
            ),
            "reset_credentials",
            {
                (0, None): "test",
                (1, "client_id"): "e469c048cf866df1",
                (1, "client_secret"): None,
            },
        )

        check_arguments(
            mock_execute(
                [
                    "principals",
                    "reset",
                    "test",
                    "--new-client-secret",
                    "e469c048cf866dfae469c048cf866df1",
                ]
            ),
            "reset_credentials",
            {
                (0, None): "test",
                (1, "client_id"): None,
                (1, "client_secret"): "e469c048cf866dfae469c048cf866df1",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "azure",
                    "--default-base-location",
                    "abfss://container@account.dfs.core.windows.net",
                    "--tenant-id",
                    "tid",
                    "--multi-tenant-app-name",
                    "my-app",
                    "--consent-url",
                    "https://consent.url",
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "AZURE",
                (0, "catalog.storage_config_info.tenant_id"): "tid",
                (0, "catalog.storage_config_info.multi_tenant_app_name"): "my-app",
                (0, "catalog.storage_config_info.consent_url"): "https://consent.url",
                (
                    0,
                    "catalog.properties.default_base_location",
                ): "abfss://container@account.dfs.core.windows.net",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "azure",
                    "--default-base-location",
                    "abfss://c@a.dfs.core.windows.net",
                    "--tenant-id",
                    "tid",
                    "--hierarchical",
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "AZURE",
                (0, "catalog.storage_config_info.tenant_id"): "tid",
                (0, "catalog.storage_config_info.hierarchical"): True,
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "s3",
                    "--default-base-location",
                    "s3://bucket/path",
                    "--role-arn",
                    "ra",
                    "--endpoint",
                    "http://s3.local:9000",
                    "--endpoint-internal",
                    "http://s3.internal:9000",
                    "--sts-endpoint",
                    "http://sts.local:9000",
                    "--no-sts",
                    "--no-kms",
                    "--path-style-access",
                    "--current-kms-key",
                    "arn:aws:kms:key",
                    "--allowed-kms-key",
                    "k1",
                    "--allowed-kms-key",
                    "k2",
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "S3",
                (0, "catalog.storage_config_info.role_arn"): "ra",
                (0, "catalog.storage_config_info.endpoint"): "http://s3.local:9000",
                (
                    0,
                    "catalog.storage_config_info.endpoint_internal",
                ): "http://s3.internal:9000",
                (
                    0,
                    "catalog.storage_config_info.sts_endpoint",
                ): "http://sts.local:9000",
                (0, "catalog.storage_config_info.sts_unavailable"): True,
                (0, "catalog.storage_config_info.kms_unavailable"): True,
                (0, "catalog.storage_config_info.path_style_access"): True,
                (0, "catalog.storage_config_info.current_kms_key"): "arn:aws:kms:key",
                (0, "catalog.storage_config_info.allowed_kms_keys"): ["k1", "k2"],
                (0, "catalog.properties.default_base_location"): "s3://bucket/path",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--storage-type",
                    "file",
                    "--default-base-location",
                    "/tmp/warehouse",
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.storage_config_info.storage_type"): "FILE",
                (0, "catalog.properties.default_base_location"): "/tmp/warehouse",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "catalogs",
                    "create",
                    "my-catalog",
                    "--type",
                    "external",
                    "--storage-type",
                    "s3",
                    "--default-base-location",
                    "s3://b/p",
                    "--role-arn",
                    "ra",
                    "--catalog-connection-type",
                    "iceberg-rest",
                    "--iceberg-remote-catalog-name",
                    "r",
                    "--catalog-uri",
                    "u",
                    "--catalog-authentication-type",
                    "implicit",
                    "--catalog-service-identity-type",
                    "aws_iam",
                    "--catalog-service-identity-iam-arn",
                    "arn:aws:iam::12345:role/polaris",
                ]
            ),
            "create_catalog",
            {
                (0, "catalog.name"): "my-catalog",
                (0, "catalog.type"): "EXTERNAL",
                (
                    0,
                    "catalog.connection_config_info.service_identity.identity_type",
                ): "AWS_IAM",
                (
                    0,
                    "catalog.connection_config_info.service_identity.iam_arn",
                ): "arn:aws:iam::12345:role/polaris",
            },
        )
        check_arguments(
            mock_execute(
                [
                    "privileges",
                    "view",
                    "revoke",
                    "--namespace",
                    "a.b",
                    "--catalog",
                    "foo",
                    "--catalog-role",
                    "bar",
                    "--view",
                    "v",
                    "VIEW_FULL_METADATA",
                ]
            ),
            "revoke_grant_from_catalog_role",
            {
                (0, None): "foo",
                (1, None): "bar",
                (2, None): False,
                (3, "grant.privilege.value"): "VIEW_FULL_METADATA",
                (3, "grant.namespace"): ["a", "b"],
                (3, "grant.view_name"): "v",
            },
        )

    def test_policies_attach_parameters_parsed_to_dict(self) -> None:
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

    def test_policies_detach_parameters_parsed_to_dict(self) -> None:
        options = Parser.parse(
            [
                "policies",
                "detach",
                "policy-name",
                "--catalog",
                "cat",
                "--attachment-type",
                "table-like",
                "--attachment-path",
                "ns.t",
                "--parameters",
                "key=value",
            ]
        )
        command = Command.from_options(options)
        command = cast(PoliciesCommand, command)
        self.assertIsInstance(command.parameters, dict)
        self.assertEqual({"key": "value"}, command.parameters)

    def test_policies_command_from_options(self) -> None:
        options = Parser.parse(
            [
                "policies",
                "create",
                "my_policy",
                "--catalog",
                "c",
                "--namespace",
                "ns1.ns2",
                "--policy-file",
                "policy.json",
                "--policy-type",
                "system.data-compaction",
                "--policy-description",
                "test description",
            ]
        )
        command = Command.from_options(options)
        command = cast(PoliciesCommand, command)
        self.assertEqual("create", command.policies_subcommand)
        self.assertEqual("c", command.catalog_name)
        self.assertEqual("ns1.ns2", command.namespace)
        self.assertEqual("my_policy", command.policy_name)
        self.assertEqual("policy.json", command.policy_file)
        self.assertEqual("system.data-compaction", command.policy_type)
        self.assertEqual("test description", command.policy_description)

    def test_namespaces_command_from_options(self) -> None:
        options = Parser.parse(
            [
                "namespaces",
                "create",
                "inner_ns",
                "--catalog",
                "my_catalog",
                "--location",
                "s3://bucket/path",
                "--property",
                "key=value",
            ]
        )
        command = Command.from_options(options)
        from apache_polaris.cli.command.namespaces import NamespacesCommand

        command = cast(NamespacesCommand, command)
        self.assertEqual("create", command.namespaces_subcommand)
        self.assertEqual("my_catalog", command.catalog)
        self.assertEqual(["inner_ns"], command.namespace)
        self.assertEqual("s3://bucket/path", command.location)
        self.assertEqual({"key": "value"}, command.properties)

    def test_namespaces_list_with_parent(self) -> None:
        options = Parser.parse(
            [
                "namespaces",
                "list",
                "--catalog",
                "my_catalog",
                "--parent",
                "parent.ns",
            ]
        )
        command = Command.from_options(options)
        from apache_polaris.cli.command.namespaces import NamespacesCommand

        command = cast(NamespacesCommand, command)
        self.assertEqual("list", command.namespaces_subcommand)
        self.assertEqual("my_catalog", command.catalog)
        self.assertEqual(["parent", "ns"], command.parent)

    def test_setup_command_from_options(self) -> None:
        options = Parser.parse(["setup", "apply", "config.yaml", "--dry-run"])
        command = Command.from_options(options)
        from apache_polaris.cli.command.setup import SetupCommand

        command = cast(SetupCommand, command)
        self.assertEqual("apply", command.setup_subcommand)
        self.assertEqual("config.yaml", command.setup_config)
        self.assertTrue(command.dry_run)

    def test_setup_export_from_options(self) -> None:
        options = Parser.parse(["setup", "export"])
        command = Command.from_options(options)
        from apache_polaris.cli.command.setup import SetupCommand

        command = cast(SetupCommand, command)
        self.assertEqual("export", command.setup_subcommand)

    def test_profiles_command_from_options(self) -> None:
        options = Parser.parse(["profiles", "create", "dev"])
        command = Command.from_options(options)
        from apache_polaris.cli.command.profiles import ProfilesCommand

        command = cast(ProfilesCommand, command)
        self.assertEqual("create", command.profiles_subcommand)
        self.assertEqual("dev", command.profile_name)

    def test_profiles_list_from_options(self) -> None:
        options = Parser.parse(["profiles", "list"])
        command = Command.from_options(options)
        from apache_polaris.cli.command.profiles import ProfilesCommand

        command = cast(ProfilesCommand, command)
        self.assertEqual("list", command.profiles_subcommand)
        self.assertIsNone(command.profile_name)


if __name__ == "__main__":
    unittest.main()
