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
from cli_test_utils import CLITestBase, INVALID_ARGS
from apache_polaris.cli.options.parser import Parser
from apache_polaris.cli.command import Command


class TestParserBasic(CLITestBase):
    def test_invalid_command(self) -> None:
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

    def test_usage(self) -> None:
        self.check_usage_output(lambda: Parser.parse(["--help"]))
        self.check_usage_output(lambda: Parser.parse(["catalogs", "--help"]))
        self.check_usage_output(lambda: Parser.parse(["catalogs", "create", "--help"]))
        self.check_usage_output(
            lambda: Parser.parse(["catalogs", "create", "something", "--help"])
        )

    def test_extended_usage(self) -> None:
        self.check_usage_output(
            lambda: Parser._build_parser().parse_args(["--help"], "input:")
        )
        self.check_usage_output(
            lambda: Parser._build_parser().parse_args(["catalogs", "--help"], "input:")
        )
        self.check_usage_output(
            lambda: Parser._build_parser().parse_args(
                ["catalogs", "create", "--help"], "input:"
            )
        )
        self.check_usage_output(
            lambda: Parser._build_parser().parse_args(
                ["catalogs", "create", "c", "--help"], "input:"
            )
        )
        self.check_usage_output(
            lambda: Parser._build_parser().parse_args(
                ["privileges", "table", "grant", "--help"], "input:"
            )
        )
        self.check_usage_output(
            lambda: Parser.parse(["catalogs", "create", "something", "--help"]),
            "input:",
        )

    def test_parse_argparse_valid_commands(self) -> None:
        # These commands are valid for parsing, but may cause errors within the command itself
        Parser.parse(["catalogs", "create", "catalog_name"])
        Parser.parse(["catalogs", "create", "catalog_name", "--type", "internal"])
        Parser.parse(["catalogs", "create", "catalog_name", "--type", "INTERNAL"])
        Parser.parse(["catalogs", "list"])
        Parser.parse(["catalogs", "get", "catalog_name"])
        Parser.parse(["principals", "list"])
        Parser.parse(["--host", "some-host", "catalogs", "list"])
        Parser.parse(
            ["--base-url", "https://customservice.com/subpath", "catalogs", "list"]
        )
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


if __name__ == "__main__":
    unittest.main()
