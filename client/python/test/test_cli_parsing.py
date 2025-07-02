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
import io
from functools import reduce
from typing import List
from unittest.mock import patch, MagicMock

from cli.command import Command
from cli.options.parser import Parser
from polaris.catalog import ApiClient
from polaris.management import PolarisDefaultApi

INVALID_ARGS = 2


class TestCliParsing(unittest.TestCase):

    def test_invalid_commands(self):
        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['not-real-command!', 'list'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'not-real-subcommand'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'create'])  # missing required input
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'create', 'catalog_name', '--type', 'BANANA'])  # invalid catalog type
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'create', 'catalog_name', '--set-property', 'foo=bar'])  # can't use --set-property on create
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'get', 'catalog_name', '--fake-flag'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'update', 'catalog_name', '--property', 'foo=bar'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['catalogs', 'create', 'catalog_name', '--type', 'EXTERNAL', '--remote-url', 'gone'])  # remote-url deprecated
            self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['principals', 'create', 'name', '--type', 'bad'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['principals', 'update', 'name', '--client-id', 'something'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['privileges', 'catalog', '--catalog', 'c', '--catalog-role', 'r', 'privilege', 'grant'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

        with self.assertRaises(SystemExit) as cm:
            Parser.parse(['privileges', '--catalog', 'c', '--catalog-role', 'r', 'catalog', 'grant', 'privilege',
                          '--namespace', 'unexpected!'])
        self.assertEqual(cm.exception.code, INVALID_ARGS)

    def _check_usage_output(self, f, needle='usage:'):
        with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout, \
                patch('sys.stderr', new_callable=io.StringIO) as mock_stderr:
            with self.assertRaises(SystemExit) as cm:
                f()
            self.assertEqual(cm.exception.code, 0)
            help_output = str(mock_stdout.getvalue())
            self.assertIn('usage:', help_output)
        print(help_output)

    def test_usage(self):
        self._check_usage_output(lambda: Parser.parse(['--help']))
        self._check_usage_output(lambda: Parser.parse(['catalogs', '--help']))
        self._check_usage_output(lambda: Parser.parse(['catalogs', 'create', '--help']))
        self._check_usage_output(lambda: Parser.parse(['catalogs', 'create', 'something', '--help']))

    def test_extended_usage(self):
        self._check_usage_output(lambda: Parser._build_parser().parse_args(['--help'], 'input:'))
        self._check_usage_output(lambda: Parser._build_parser().parse_args(['catalogs', '--help'], 'input:'))
        self._check_usage_output(lambda: Parser._build_parser().parse_args(['catalogs', 'create', '--help'], 'input:'))
        self._check_usage_output(lambda: Parser._build_parser().parse_args([
            'catalogs', 'create', 'c', '--help'], 'input:'))
        self._check_usage_output(lambda: Parser._build_parser().parse_args([
            'privileges', 'table', 'grant', '--help'], 'input:'))
        self._check_usage_output(lambda: Parser.parse(['catalogs', 'create', 'something', '--help']), 'input:')

    def test_parsing_valid_commands(self):
        Parser.parse(['catalogs', 'create', 'catalog_name'])
        Parser.parse(['catalogs', 'create', 'catalog_name', '--type', 'internal'])
        Parser.parse(['catalogs', 'create', 'catalog_name', '--type', 'INTERNAL'])
        Parser.parse(['catalogs', 'list'])
        Parser.parse(['catalogs', 'get', 'catalog_name'])
        Parser.parse(['principals', 'list'])
        Parser.parse(['--host', 'some-host', 'catalogs', 'list'])
        Parser.parse(['--base-url', 'https://customservice.com/subpath', 'catalogs', 'list'])
        Parser.parse(['privileges', 'catalog', 'grant', '--catalog', 'foo', '--catalog-role', 'bar', 'TABLE_READ_DATA'])
        Parser.parse(['privileges', 'table', 'grant', '--catalog', 'foo', '--catalog-role', 'bar',
                      '--namespace', 'n', '--table', 't', 'TABLE_READ_DATA'])
        Parser.parse(['privileges', 'table', 'revoke', '--catalog', 'foo', '--catalog-role', 'bar',
                      '--namespace', 'n', '--table', 't', 'TABLE_READ_DATA'])

    # These commands are valid for parsing, but may cause errors within the command itself
    def test_parse_argparse_valid_commands(self):
        Parser.parse(['catalogs', 'create', 'catalog_name', '--type', 'internal'])
        Parser.parse(['privileges', 'table', 'grant',
                      '--namespace', 'n', '--table', 't', 'TABLE_READ_DATA'])
        Parser.parse(['privileges', 'catalog', 'grant', '--catalog', 'c', '--catalog-role', 'r', 'fake-privilege'])

    def test_commands(self):

        def build_mock_client():
            client = MagicMock(spec=PolarisDefaultApi)
            client.call_tracker = dict()

            def capture_method(method_name):
                def _capture(*args, **kwargs):
                    client.call_tracker['_method'] = method_name
                    for i, arg in enumerate(args):
                        if arg is not None:
                            client.call_tracker[i] = arg

                return _capture

            for method_name in dir(client):
                if callable(getattr(client, method_name)) and not method_name.startswith('__'):
                    setattr(client, method_name,
                            MagicMock(name=method_name, side_effect=capture_method(method_name)))
            return client
        mock_client = build_mock_client()

        def mock_execute(input: List[str]):
            mock_client.call_tracker = dict()

            # Assuming Parser and Command are used to parse input and generate commands
            options = Parser.parse(input)
            command = Command.from_options(options)

            try:
                command.execute(mock_client)
            except AttributeError as e:
                # Some commands may fail due to the mock, but the results should still match expectations
                print(f'Suppressed error: {e}')
            return mock_client.call_tracker

        def check_exception(f, exception_str):
            throws = True
            try:
                f()
                throws = False
            except Exception as e:
                self.assertIn(exception_str, str(e))
            self.assertTrue(throws, 'Exception should be raised')

        def check_arguments(result, method_name, args=dict()):
            self.assertEqual(method_name, result['_method'])

            def get(obj, arg_string):
                attributes = arg_string.split('.')
                return reduce(getattr, attributes, obj)

            for arg, value in args.items():
                index, path = arg
                if path is not None:
                    self.assertEqual(value, get(result[index], path))
                else:
                    self.assertEqual(value, result[index])

        # Test various failing commands:
        check_exception(lambda: mock_execute(['catalogs', 'create', 'my-catalog']),
                        '--storage-type')
        check_exception(lambda: mock_execute(['catalogs', 'create', 'my-catalog', '--storage-type', 'gcs']),
                        '--default-base-location')
        check_exception(lambda: mock_execute(['catalog-roles', 'get', 'foo']),
                        '--catalog')
        check_exception(lambda: mock_execute(['catalogs', 'update', 'foo', '--set-property', 'bad-format']),
                        'bad-format')
        check_exception(lambda: mock_execute(['privileges', 'catalog', 'grant',
                                              '--catalog', 'foo', '--catalog-role', 'bar', 'TABLE_READ_MORE_BOOKS']),
                        'catalog privilege: TABLE_READ_MORE_BOOKS')
        check_exception(lambda: mock_execute(['catalogs', 'create', 'my-catalog', '--storage-type', 'gcs',
                                              '--allowed-location', 'a', '--allowed-location', 'b',
                                              '--role-arn', 'ra', '--default-base-location', 'x']),
                        'gcs')

        # Test various correct commands:
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--storage-type', 'gcs', '--default-base-location', 'x']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.storage_config_info.storage_type'): 'GCS',
                (0, 'catalog.properties.default_base_location'): 'x',
            })
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--type', 'external',
                          '--storage-type', 'gcs', '--default-base-location', 'dbl']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.type'): 'EXTERNAL',
            })
        check_arguments(
            mock_execute([
                'catalogs', 'create', 'my-catalog', '--storage-type', 's3',
                '--allowed-location', 'a', '--allowed-location', 'b', '--role-arn', 'ra',
                '--external-id', 'ei', '--default-base-location', 'x']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.storage_config_info.storage_type'): 'S3',
                (0, 'catalog.properties.default_base_location'): 'x',
                (0, 'catalog.storage_config_info.allowed_locations'): ['a', 'b'],
            })
        check_arguments(
            mock_execute([
                'catalogs', 'create', 'my-catalog', '--storage-type', 's3',
                '--allowed-location', 'a', '--role-arn', 'ra', '--region', 'us-west-2',
                '--external-id', 'ei', '--default-base-location', 'x']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.storage_config_info.storage_type'): 'S3',
                (0, 'catalog.properties.default_base_location'): 'x',
                (0, 'catalog.storage_config_info.allowed_locations'): ['a'],
                (0, 'catalog.storage_config_info.region'): 'us-west-2',
            })
        check_arguments(
            mock_execute([
                'catalogs', 'create', 'my-catalog', '--storage-type', 'gcs',
                '--allowed-location', 'a', '--allowed-location', 'b',
                '--service-account', 'sa', '--default-base-location', 'x']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.storage_config_info.storage_type'): 'GCS',
                (0, 'catalog.properties.default_base_location'): 'x',
                (0, 'catalog.storage_config_info.allowed_locations'): ['a', 'b'],
                (0, 'catalog.storage_config_info.gcs_service_account'): 'sa',
            })
        check_arguments(mock_execute(['catalogs', 'list']), 'list_catalogs')
        check_arguments(mock_execute([
            '--base-url', 'https://customservice.com/subpath', 'catalogs', 'list']), 'list_catalogs')
        check_arguments(mock_execute(['catalogs', 'delete', 'foo']), 'delete_catalog', {
            (0, None): 'foo',
        })
        check_arguments(mock_execute(['catalogs', 'get', 'foo']), 'get_catalog', {
            (0, None): 'foo',
        })
        check_arguments(
            mock_execute(['catalogs', 'update', 'foo', '--default-base-location', 'x']),
            'get_catalog', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['catalogs', 'update', 'foo', '--set-property', 'key=value']),
            'get_catalog', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['catalogs', 'update', 'foo', '--set-property', 'listkey=k1=v1,k2=v2']),
            'get_catalog', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['catalogs', 'update', 'foo', '--remove-property', 'key']),
            'get_catalog', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['catalogs', 'update', 'foo', '--set-property', 'key=value', '--default-base-location', 'x']),
            'get_catalog', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute([
                'catalogs', 'update', 'foo', '--set-property', 'key=value',
                '--default-base-location', 'x', '--region', 'us-west-1']),
            'get_catalog', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principals', 'create', 'foo', '--property', 'key=value']),
            'create_principal', {
                (0, 'principal.name'): 'foo',
                (0, 'principal.client_id'): None,
                (0, 'principal.properties'): {'key': 'value'},
            })
        check_arguments(
            mock_execute(['principals', 'delete', 'foo']),
            'delete_principal', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principals', 'get', 'foo']),
            'get_principal', {
                (0, None): 'foo',
            })
        check_arguments(mock_execute(['principals', 'list']), 'list_principals')
        check_arguments(
            mock_execute(['principals', 'rotate-credentials', 'foo']),
            'rotate_credentials', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principals', 'update', 'foo', '--set-property', 'key=value']),
            'get_principal', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principals', 'update', 'foo', '--remove-property', 'key']),
            'get_principal', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'create', 'foo']),
            'create_principal_role', {
                (0, 'principal_role.name'): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'delete', 'foo']),
            'delete_principal_role', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'delete', 'foo']),
            'delete_principal_role', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'get', 'foo']),
            'get_principal_role', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'get', 'foo']),
            'get_principal_role', {
                (0, None): 'foo',
            })
        check_arguments(mock_execute(['principal-roles', 'list']), 'list_principal_roles')
        check_arguments(
            mock_execute(['principal-roles', 'list', '--principal', 'foo']),
            'list_principal_roles_assigned', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'update', 'foo', '--set-property', 'key=value']),
            'get_principal_role', {
                (0, None): 'foo'
            })
        check_arguments(
            mock_execute(['principal-roles', 'update', 'foo', '--remove-property', 'key']),
            'get_principal_role', {
                (0, None): 'foo',
            })
        check_arguments(
            mock_execute(['principal-roles', 'grant', 'bar', '--principal', 'foo']),
            'assign_principal_role', {
                (0, None): 'foo',
                (1, 'principal_role.name'): 'bar',
            })
        check_arguments(
            mock_execute(['principal-roles', 'revoke', 'bar', '--principal', 'foo']),
            'revoke_principal_role', {
                (0, None): 'foo',
                (1, None): 'bar',
            })
        check_arguments(
            mock_execute(
                ['catalog-roles', 'create', 'foo', '--catalog', 'bar', '--property', 'key=value']),
            'create_catalog_role', {
                (0, None): 'bar',
                (1, 'catalog_role.name'): 'foo',
                (1, 'catalog_role.properties'): {'key': 'value'},
            })
        check_arguments(
            mock_execute(
                ['catalog-roles', 'delete', 'foo', '--catalog', 'bar']),
            'delete_catalog_role', {
                (0, None): 'bar',
                (1, None): 'foo',
            })
        check_arguments(
            mock_execute(
                ['catalog-roles', 'get', 'foo', '--catalog', 'bar']),
            'get_catalog_role', {
                (0, None): 'bar',
                (1, None): 'foo',
            })
        check_arguments(mock_execute(
            ['catalog-roles', 'list', 'foo']),
            'list_catalog_roles', {
                (0, None): 'foo',
            })
        check_arguments(mock_execute(
            ['catalog-roles', 'list', 'foo', '--principal-role', 'bar']),
            'list_catalog_roles_for_principal_role', {
                (0, None): 'bar',
                (1, None): 'foo',
            })
        check_arguments(mock_execute(
            ['catalog-roles', 'update', 'foo', '--catalog', 'bar', '--set-property', 'key=value']),
            'get_catalog_role', {
                (0, None): 'bar',
                (1, None): 'foo',
            })
        check_arguments(mock_execute(
            ['catalog-roles', 'update', 'foo', '--catalog', 'bar', '--remove-property', 'key']),
            'get_catalog_role', {
                (0, None): 'bar',
                (1, None): 'foo',
            })
        check_arguments(
            mock_execute(['catalog-roles', 'grant', '--principal-role', 'foo', '--catalog', 'bar', 'baz']),
            'assign_catalog_role_to_principal_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, 'catalog_role.name'): 'baz',
            })
        check_arguments(
            mock_execute(['catalog-roles', 'revoke', '--principal-role', 'foo', '--catalog', 'bar', 'baz']),
            'revoke_catalog_role_from_principal_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, None): 'baz',
            })
        check_arguments(
            mock_execute(
                ['privileges', 'catalog', 'grant', '--catalog', 'foo', '--catalog-role', 'bar', 'TABLE_READ_DATA']),
            'add_grant_to_catalog_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, 'grant.privilege.value'): 'TABLE_READ_DATA',
            })
        check_arguments(
            mock_execute(
                ['privileges', 'catalog', 'revoke', '--catalog', 'foo', '--catalog-role', 'bar', 'TABLE_READ_DATA']),
            'revoke_grant_from_catalog_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, None): False,
                (3, 'grant.privilege.value'): 'TABLE_READ_DATA',
            })
        check_arguments(
            mock_execute(
                ['privileges', 'namespace', 'grant', '--namespace', 'a.b.c', '--catalog', 'foo',
                 '--catalog-role', 'bar', 'TABLE_READ_DATA']),
            'add_grant_to_catalog_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, 'grant.privilege.value'): 'TABLE_READ_DATA',
                (2, 'grant.namespace'): ['a', 'b', 'c'],
            })
        check_arguments(
            mock_execute(
                ['privileges', 'table', 'grant', '--namespace', 'a.b.c',
                 '--table', 't', '--catalog', 'foo', '--catalog-role', 'bar', 'TABLE_READ_DATA']),
            'add_grant_to_catalog_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, 'grant.privilege.value'): 'TABLE_READ_DATA',
                (2, 'grant.namespace'): ['a', 'b', 'c'],
                (2, 'grant.table_name'): 't',
            })
        check_arguments(
            mock_execute(
                ['privileges', 'table', 'revoke', '--namespace', 'a.b.c', '--catalog', 'foo', '--catalog-role', 'bar',
                 '--table', 't', '--cascade', 'TABLE_READ_DATA']),
            'revoke_grant_from_catalog_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, None): True,
                (3, 'grant.privilege.value'): 'TABLE_READ_DATA',
                (3, 'grant.namespace'): ['a', 'b', 'c'],
                (3, 'grant.table_name'): 't',
            })
        check_arguments(
            mock_execute(
                ['privileges', 'view', 'grant', '--namespace', 'a.b.c', '--catalog', 'foo', '--catalog-role', 'bar',
                 '--view', 'v', 'VIEW_FULL_METADATA']),
            'add_grant_to_catalog_role', {
                (0, None): 'foo',
                (1, None): 'bar',
                (2, 'grant.privilege.value'): 'VIEW_FULL_METADATA',
                (2, 'grant.namespace'): ['a', 'b', 'c'],
                (2, 'grant.view_name'): 'v',
            })
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--type', 'external',
                          '--storage-type', 'gcs', '--default-base-location', 'dbl',
                          '--catalog-connection-type', 'hadoop', '--hadoop-warehouse', 'h',
                          '--catalog-uri', 'u', '--catalog-authentication-type', 'bearer',
                          '--catalog-bearer-token', 'b']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.type'): 'EXTERNAL',
                (0, 'catalog.connection_config_info.connection_type'): 'HADOOP',
                (0, 'catalog.connection_config_info.warehouse'): 'h',
                (0, 'catalog.connection_config_info.uri'): 'u',
            })
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--type', 'external',
                          '--storage-type', 'gcs', '--default-base-location', 'dbl',
                          '--catalog-connection-type', 'iceberg-rest', '--iceberg-remote-catalog-name', 'i',
                          '--catalog-uri', 'u', '--catalog-authentication-type', 'bearer',
                          '--catalog-bearer-token', 'b']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.type'): 'EXTERNAL',
                (0, 'catalog.connection_config_info.connection_type'): 'ICEBERG_REST',
                (0, 'catalog.connection_config_info.remote_catalog_name'): 'i',
                (0, 'catalog.connection_config_info.uri'): 'u',
            })
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--type', 'external',
                          '--storage-type', 'gcs', '--default-base-location', 'dbl',
                          '--catalog-connection-type', 'hadoop', '--hadoop-warehouse', 'h',
                          '--catalog-authentication-type', 'oauth',
                          '--catalog-token-uri', 'u', '--catalog-client-id', 'i',
                          '--catalog-client-secret', 'k', '--catalog-client-scope', 's1',
                          '--catalog-client-scope', 's2']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.type'): 'EXTERNAL',
                (0, 'catalog.connection_config_info.connection_type'): 'HADOOP',
                (0, 'catalog.connection_config_info.warehouse'): 'h',
                (0, 'catalog.connection_config_info.authentication_parameters.authentication_type'): 'OAUTH',
                (0, 'catalog.connection_config_info.authentication_parameters.token_uri'): 'u',
                (0, 'catalog.connection_config_info.authentication_parameters.client_id'): 'i',
                (0, 'catalog.connection_config_info.authentication_parameters.scopes'): ['s1', 's2'],
            })
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--type', 'external',
                          '--storage-type', 'gcs', '--default-base-location', 'dbl',
                          '--catalog-connection-type', 'iceberg-rest', '--iceberg-remote-catalog-name', 'i',
                          '--catalog-uri', 'u', '--catalog-authentication-type', 'sigv4',
                          '--catalog-role-arn', 'a', '--catalog-signing-region', 's']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.type'): 'EXTERNAL',
                (0, 'catalog.connection_config_info.connection_type'): 'ICEBERG_REST',
                (0, 'catalog.connection_config_info.remote_catalog_name'): 'i',
                (0, 'catalog.connection_config_info.uri'): 'u',
                (0, 'catalog.connection_config_info.authentication_parameters.role_arn'): 'a',
                (0, 'catalog.connection_config_info.authentication_parameters.signing_region'): 's',
            })
        check_arguments(
            mock_execute(['catalogs', 'create', 'my-catalog', '--type', 'external',
                          '--storage-type', 'gcs', '--default-base-location', 'dbl',
                          '--catalog-connection-type', 'iceberg-rest', '--iceberg-remote-catalog-name', 'i',
                          '--catalog-uri', 'u', '--catalog-authentication-type', 'sigv4',
                          '--catalog-role-arn', 'a', '--catalog-signing-region', 's',
                          '--catalog-role-session-name', 'n', '--catalog-external-id', 'i',
                          '--catalog-signing-name', 'g']),
            'create_catalog', {
                (0, 'catalog.name'): 'my-catalog',
                (0, 'catalog.type'): 'EXTERNAL',
                (0, 'catalog.connection_config_info.connection_type'): 'ICEBERG_REST',
                (0, 'catalog.connection_config_info.remote_catalog_name'): 'i',
                (0, 'catalog.connection_config_info.uri'): 'u',
                (0, 'catalog.connection_config_info.authentication_parameters.role_arn'): 'a',
                (0, 'catalog.connection_config_info.authentication_parameters.signing_region'): 's',
                (0, 'catalog.connection_config_info.authentication_parameters.role_session_name'): 'n',
                (0, 'catalog.connection_config_info.authentication_parameters.external_id'): 'i',
                (0, 'catalog.connection_config_info.authentication_parameters.signing_name'): 'g',
            })


if __name__ == '__main__':
    unittest.main()