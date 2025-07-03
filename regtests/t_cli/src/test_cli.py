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
import json
import os
import random
import requests
import string
import subprocess
import sys
from typing import Callable

CLI_PYTHONPATH = f'{os.path.dirname(os.path.abspath(__file__))}/../../../client/python'
ROLE_ARN = 'arn:aws:iam::123456789012:role/my-role'
POLARIS_HOST = os.getenv('POLARIS_HOST', 'localhost')
POLARIS_URL = f'http://{POLARIS_HOST}:8181/api/catalog/v1/oauth/tokens'

def get_salt(length=8) -> str:
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def root_cli(*args):
    return cli(os.getenv('REGTEST_ROOT_BEARER_TOKEN'))(*args)


def cli(access_token):
    def cli_inner(*args) -> Callable[[], str]:
        def f() -> str:
            result = subprocess.run([
                'bash',
                f'{CLI_PYTHONPATH}/../../polaris',
                '--access-token',
                access_token,
                '--host',
                POLARIS_HOST,
                *args],
                capture_output=True,
                text=True
            )
            print(result)
            if result.returncode != 0:
                raise Exception(result.stderr)
            return result.stdout
        return f
    return cli_inner


def check_output(f, checker: Callable[[str], bool]):
    assert checker(f())


def check_exception(f, exception_str):
    throws = True
    try:
        f()
        throws = False
    except Exception as e:
        assert exception_str in str(e)
    assert throws


def get_token(client_id, client_secret):
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'PRINCIPAL_ROLE:ALL'
    }

    response = requests.post(POLARIS_URL, data=data)

    if response.status_code != 200 or 'access_token' not in response.json():
        raise Exception("Failed to retrieve token")

    return response.json()['access_token']


def test_quickstart_flow():
    """
    Basic CLI test - create a catalog, create a principal, and grant the principal access to the catalog.
    """

    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a catalog:
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            's3',
            '--role-arn',
            ROLE_ARN,
            '--default-base-location',
            f's3://fake-location-{SALT}',
            f'test_cli_catalog_{SALT}'), checker=lambda s: s == '')
        check_output(root_cli('catalogs', 'list'),
                     checker=lambda s: f'test_cli_catalog_{SALT}' in s)
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s)

        # Create a new user:
        credentials = root_cli('principals', 'create', f'test_cli_user_{SALT}')()
        check_output(root_cli('principals', 'list'), checker=lambda s: f'test_cli_user_{SALT}' in s)
        credentials = json.loads(credentials)
        assert 'clientId' in credentials
        assert 'clientSecret' in credentials
        user_token = get_token(credentials['clientId'], credentials['clientSecret'])

        # User initially has no catalog access:
        check_exception(cli(user_token)('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                        exception_str='not authorized')

        # Grant user access:
        check_output(
            root_cli('principal-roles', 'create', f'test_cli_p_role_{SALT}'),
            checker=lambda s: s == '')
        check_output(root_cli(
            'catalog-roles', 'create', '--catalog', f'test_cli_catalog_{SALT}', f'test_cli_c_role_{SALT}'),
            checker=lambda s: s == '')
        check_output(root_cli(
            'principal-roles', 'grant', '--principal', f'test_cli_user_{SALT}', f'test_cli_p_role_{SALT}'),
            checker=lambda s: s == '')
        check_output(root_cli(
            'catalog-roles',
            'grant',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--principal-role',
            f'test_cli_p_role_{SALT}',
            f'test_cli_c_role_{SALT}'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'privileges',
            'catalog',
            'grant',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--catalog-role',
            f'test_cli_c_role_{SALT}',
            'CATALOG_MANAGE_CONTENT'
        ), checker=lambda s: s == '')

        # User now has catalog access:
        check_output(cli(user_token)('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s)
        check_output(cli(user_token)(
            'namespaces',
            'create',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'test_cli_namespace_{SALT}',
            '--property',
            'foo=bar',
            '--location',
            's3://custom-namespace-location'
        ), checker=lambda s: s == '')
        check_output(cli(user_token)('namespaces', 'list', '--catalog', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: f'test_cli_namespace_{SALT}' in s)
        check_output(cli(user_token)(
            'namespaces',
            'get',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'test_cli_namespace_{SALT}'
        ), checker=lambda s: 's3://custom-namespace-location' in s and '"foo": "bar"' in s)
        check_output(cli(user_token)(
            'namespaces',
            'delete',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'test_cli_namespace_{SALT}'
        ), checker=lambda s: s == '')
        check_output(cli(user_token)('namespaces', 'list', '--catalog', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: f'test_cli_namespace_{SALT}' not in s)

    finally:
        sys.path.pop(0)
    pass


def test_catalog_storage_config():
    """
    Tests specific to storage configs of catalogs across different cloud object stores
    """

    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:
        # Create S3 catalog
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            's3',
            '--role-arn',
            ROLE_ARN,
            '--default-base-location',
            f's3://fake-location-{SALT}',
            '--external-id',
            'custom-external-id-123',
            f'test_cli_catalog_aws_{SALT}'), checker=lambda s: s == '')
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_aws_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s and
                         'custom-external-id-123' in s)

        # Create Azure catalog
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            'azure',
            '--default-base-location',
            f'abfss://fake-container-{SALT}@acct1.dfs.core.windows.net',
            '--tenant-id',
            'tenant123.onmicrosoft.com',
            '--multi-tenant-app-name',
            'mtapp123',
            '--consent-url',
            'https://fake-consent-url',
            f'test_cli_catalog_azure_{SALT}'), checker=lambda s: s == '')
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_azure_{SALT}'),
                     checker=lambda s: 'abfss://fake-container' in s and
                         'tenant123.onmicrosoft.com' in s and
                         'mtapp123' in s and
                         'https://fake-consent-url' in s)

        # Create GCS catalog
        # TODO: Once catalog capabilities are formalized in an API, condition this
        # GCP scenario on whether the catalog under test has GCP root credentials available.
        # In the meantime, since the credentials aren't actually used, just initialized,
        # the following lines can be uncommented if running locally while using
        # DEVSHELL_CLIENT_PORT to create a nonfunctional fake GCP credential supplier:
        #
        # DEVSHELL_CLIENT_PORT=0 ./gradlew runApp
        # check_output(root_cli(
        #     'catalogs',
        #     'create',
        #     '--storage-type',
        #     'gcs',
        #     '--default-base-location',
        #     f'gs://fake-location-{SALT}',
        #     '--service-account',
        #     'service-acct-123',
        #     f'test_cli_catalog_gcp_{SALT}'), checker=lambda s: s == '')
        # check_output(root_cli('catalogs', 'get', f'test_cli_catalog_gcp_{SALT}'),
        #              checker=lambda s: 'gs://fake-location' in s and
        #                  'service-acct-123' in s)

    finally:
        sys.path.pop(0)
    pass


def test_update_catalog():
    """
    Test updating properties on a catalog
    """
    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a catalog:
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            's3',
            '--role-arn',
            ROLE_ARN,
            '--default-base-location',
            f's3://fake-location-{SALT}',
            f'test_cli_catalog_{SALT}'), checker=lambda s: s == '')
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s)

        # Update by adding a property
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'foo=bar'
        ), checker=lambda s: s == '')

        # Make sure the base location didn't get clobbered, and the new property is present.
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s and '"foo": "bar"' in s)

        # Update by changing default-base-location
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--default-base-location',
            f's3://new-location-{SALT}'
        ), checker=lambda s: s == '')

        # Check for new location, and make sure the custom property is still there.
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://new-location' in s and '"foo": "bar"' in s)

        # Update by adding a second property
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'prop2=222'
        ), checker=lambda s: s == '')

        # Make sure both custom properties are present even though we only added the second one
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: '"prop2": "222"' in s and '"foo": "bar"' in s)

        # Update by modifying a property and adding a property at the same time
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'prop2=two',
            '--set-property',
            'prop3=333'
        ), checker=lambda s: s == '')

        # Check all three custom properties
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: '"prop2": "two"' in s and '"foo": "bar"' in s and '"prop3": "333"' in s)

        # Update one property that will be removed in the same command, one that will not be removed,
        # add one new property, remove one of the properties that is not updated, and remove a
        # nonexistent property
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'prop2=2222',
            '--set-property',
            'prop3=3333',
            '--set-property',
            'prop4=4444',
            '--remove-property',
            'foo',
            '--remove-property',
            'prop2',
            '--remove-property',
            'nonexistent'
        ), checker=lambda s: s == '')

        # One updated property and one new property remain after removals
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 'foo' not in s and 'prop2' not in s and
                                       '"prop3": "3333"' in s and '"prop4": "4444"' in s)

        # Update to add a property whose value is a key-value list
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'listprop=k1=v1,k2=v2'
        ), checker=lambda s: s == '')

        # Previous properties still exist, and the new property is parsed properly
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: '"prop3": "3333"' in s and '"prop4": "4444"' in s and
                                       '"listprop": "k1=v1,k2=v2"' in s)

        # Update to set a region
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--region',
            'new-test-region'
        ), checker=lambda s: s == '')

        # Original fake-location should still be present in storage_config_info.allowed_locations
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location-' in s and '"region": "new-test-region"' in s)

        # Update to add an allowed location
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--allowed-location',
            f's3://extra-allowed-location-{SALT}'
        ), checker=lambda s: s == '')

        # All allowed locations present and region still present
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location-' in s and
                                       's3://extra-allowed-location-' in s and
                                       '"region": "new-test-region"' in s)

        # Add allowed location and change region at same time
        check_output(root_cli(
            'catalogs',
            'update',
            f'test_cli_catalog_{SALT}',
            '--allowed-location',
            f's3://fourth-allowed-location-{SALT}',
            '--region',
            'us-east-2'
        ), checker=lambda s: s == '')

        # All allowed locations present and new region set
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location-' in s and
                                       's3://extra-allowed-location-' in s and
                                       's3://fourth-allowed-location-' in s and
                                       '"region": "us-east-2"' in s)

    finally:
        sys.path.pop(0)
    pass

def test_update_principal():
    """
    Test updating properties on a principal
    """
    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a principal:
        credentials = root_cli('principals', 'create', f'test_cli_user_{SALT}', '--property', 'foo=bar')()
        check_output(root_cli('principals', 'get', f'test_cli_user_{SALT}'),
                     checker=lambda s: '"foo": "bar"' in s)

        # Update by adding a second property
        check_output(root_cli(
            'principals',
            'update',
            f'test_cli_user_{SALT}',
            '--set-property',
            'prop2=222'
        ), checker=lambda s: s == '')

        # Make sure both custom properties are present even though we only added the second one
        check_output(root_cli('principals', 'get', f'test_cli_user_{SALT}'),
                     checker=lambda s: '"prop2": "222"' in s and '"foo": "bar"' in s)

        # Update by modifying a property and adding a property at the same time
        check_output(root_cli(
            'principals',
            'update',
            f'test_cli_user_{SALT}',
            '--set-property',
            'prop2=two',
            '--set-property',
            'prop3=333'
        ), checker=lambda s: s == '')

        # Check all three custom properties
        check_output(root_cli('principals', 'get', f'test_cli_user_{SALT}'),
                     checker=lambda s: '"prop2": "two"' in s and '"foo": "bar"' in s and '"prop3": "333"' in s)

        # Update one property that will be removed in the same command, one that will not be removed,
        # add one new property, remove one of the properties that is not updated, and remove a
        # nonexistent property
        check_output(root_cli(
            'principals',
            'update',
            f'test_cli_user_{SALT}',
            '--set-property',
            'prop2=2222',
            '--set-property',
            'prop3=3333',
            '--set-property',
            'prop4=4444',
            '--remove-property',
            'foo',
            '--remove-property',
            'prop2',
            '--remove-property',
            'nonexistent'
        ), checker=lambda s: s == '')

        # One updated property and one new property remain after removals
        check_output(root_cli('principals', 'get', f'test_cli_user_{SALT}'),
                     checker=lambda s: 'foo' not in s and 'prop2' not in s and
                                       '"prop3": "3333"' in s and '"prop4": "4444"' in s)

    finally:
        sys.path.pop(0)
    pass

def test_update_principal_role():
    """
    Test updating properties on a principal_role
    """
    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a principal_role:
        credentials = root_cli('principal-roles', 'create', f'test_cli_p_role_{SALT}', '--property', 'foo=bar')()
        check_output(root_cli('principal-roles', 'get', f'test_cli_p_role_{SALT}'),
                     checker=lambda s: '"foo": "bar"' in s)

        # Update by adding a second property
        check_output(root_cli(
            'principal-roles',
            'update',
            f'test_cli_p_role_{SALT}',
            '--set-property',
            'prop2=222'
        ), checker=lambda s: s == '')

        # Make sure both custom properties are present even though we only added the second one
        check_output(root_cli('principal-roles', 'get', f'test_cli_p_role_{SALT}'),
                     checker=lambda s: '"prop2": "222"' in s and '"foo": "bar"' in s)

        # Update by modifying a property and adding a property at the same time
        check_output(root_cli(
            'principal-roles',
            'update',
            f'test_cli_p_role_{SALT}',
            '--set-property',
            'prop2=two',
            '--set-property',
            'prop3=333'
        ), checker=lambda s: s == '')

        # Check all three custom properties
        check_output(root_cli('principal-roles', 'get', f'test_cli_p_role_{SALT}'),
                     checker=lambda s: '"prop2": "two"' in s and '"foo": "bar"' in s and '"prop3": "333"' in s)

        # Update one property that will be removed in the same command, one that will not be removed,
        # add one new property, remove one of the properties that is not updated, and remove a
        # nonexistent property
        check_output(root_cli(
            'principal-roles',
            'update',
            f'test_cli_p_role_{SALT}',
            '--set-property',
            'prop2=2222',
            '--set-property',
            'prop3=3333',
            '--set-property',
            'prop4=4444',
            '--remove-property',
            'foo',
            '--remove-property',
            'prop2',
            '--remove-property',
            'nonexistent'
        ), checker=lambda s: s == '')

        # One updated property and one new property remain after removals
        check_output(root_cli('principal-roles', 'get', f'test_cli_p_role_{SALT}'),
                     checker=lambda s: 'foo' not in s and 'prop2' not in s and
                                       '"prop3": "3333"' in s and '"prop4": "4444"' in s)

    finally:
        sys.path.pop(0)
    pass

def test_update_catalog_role():
    """
    Test updating properties on a catalog_role
    """
    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a catalog:
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            's3',
            '--role-arn',
            ROLE_ARN,
            '--default-base-location',
            f's3://fake-location-{SALT}',
            f'test_cli_catalog_{SALT}'), checker=lambda s: s == '')

        # Create a catalog_role:
        credentials = root_cli(
            'catalog-roles',
            'create',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--property',
            'foo=bar')()
        check_output(root_cli(
            'catalog-roles',
            'get',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
        ), checker=lambda s: '"foo": "bar"' in s)

        # Update by adding a second property
        check_output(root_cli(
            'catalog-roles',
            'update',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'prop2=222'
        ), checker=lambda s: s == '')

        # Make sure both custom properties are present even though we only added the second one
        check_output(root_cli(
            'catalog-roles',
            'get',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
        ), checker=lambda s: '"prop2": "222"' in s and '"foo": "bar"' in s)

        # Update by modifying a property and adding a property at the same time
        check_output(root_cli(
            'catalog-roles',
            'update',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'prop2=two',
            '--set-property',
            'prop3=333'
        ), checker=lambda s: s == '')

        # Check all three custom properties
        check_output(root_cli(
            'catalog-roles',
            'get',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
        ), checker=lambda s: '"prop2": "two"' in s and '"foo": "bar"' in s and '"prop3": "333"' in s)

        # Update one property that will be removed in the same command, one that will not be removed,
        # add one new property, remove one of the properties that is not updated, and remove a
        # nonexistent property
        check_output(root_cli(
            'catalog-roles',
            'update',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--set-property',
            'prop2=2222',
            '--set-property',
            'prop3=3333',
            '--set-property',
            'prop4=4444',
            '--remove-property',
            'foo',
            '--remove-property',
            'prop2',
            '--remove-property',
            'nonexistent'
        ), checker=lambda s: s == '')

        # One updated property and one new property remain after removals
        check_output(root_cli(
            'catalog-roles',
            'get',
            f'test_cli_c_role_{SALT}',
            '--catalog',
            f'test_cli_catalog_{SALT}',
        ), checker=lambda s: 'foo' not in s and 'prop2' not in s and
                             '"prop3": "3333"' in s and '"prop4": "4444"' in s)

    finally:
        sys.path.pop(0)
    pass

def test_nested_namespace():
    """
    Test creating and managing deeply nested namespaces.
    """

    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a catalog:
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            's3',
            '--role-arn',
            ROLE_ARN,
            '--default-base-location',
            f's3://fake-location-{SALT}',
            f'test_cli_catalog_{SALT}'), checker=lambda s: s == '')
        check_output(root_cli('catalogs', 'list'),
                     checker=lambda s: f'test_cli_catalog_{SALT}' in s)
        check_output(root_cli('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s)

        # Create some namespaces:
        check_output(root_cli(
            'namespaces',
            'create',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'a_{SALT}'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'namespaces',
            'create',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'a_{SALT}.b_{SALT}'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'namespaces',
            'create',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'a_{SALT}.b_{SALT}.c_{SALT}'
        ), checker=lambda s: s == '')

        # List namespaces:
        check_output(root_cli('namespaces', 'list', '--catalog', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: f'a_{SALT}' in s and f'b_{SALT}' not in s)
        check_output(root_cli(
            'namespaces',
            'list',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--parent',
            f'a_{SALT}'
        ), checker=lambda s: f'a_{SALT}.b_{SALT}' in s)

        # a.b.c exists, and a.b can't be deleted while non-empty
        check_output(root_cli(
            'namespaces',
            'get',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'a_{SALT}.b_{SALT}.c_{SALT}'
        ), checker=lambda s: f'"a_{SALT}", "b_{SALT}", "c_{SALT}"' in s)
        check_exception(root_cli(
            'namespaces',
            'delete',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'a_{SALT}.b_{SALT}'
        ), exception_str='not empty')
    finally:
        sys.path.pop(0)
    pass


def test_list_privileges():
    """
    Test creating and managing deeply nested namespaces.
    """

    SALT = get_salt()
    sys.path.insert(0, CLI_PYTHONPATH)
    try:

        # Create a catalog, namespace, and principal:
        check_output(root_cli(
            'catalogs',
            'create',
            '--storage-type',
            's3',
            '--role-arn',
            ROLE_ARN,
            '--default-base-location',
            f's3://fake-location-{SALT}',
            f'test_cli_catalog_{SALT}'), checker=lambda s: s == '')
        check_output(root_cli(
            'namespaces',
            'create',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'a_{SALT}'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'principals',
            'create',
            f'test_cli_user_{SALT}'
        ), checker=lambda s: s != '')

        # Grant the principal some privileges:
        check_output(
            root_cli('principal-roles', 'create', f'test_cli_p_role_{SALT}'),
            checker=lambda s: s == '')
        check_output(root_cli(
            'catalog-roles', 'create', '--catalog', f'test_cli_catalog_{SALT}', f'test_cli_c_role_{SALT}'),
            checker=lambda s: s == '')
        check_output(root_cli(
            'principal-roles', 'grant', '--principal', f'test_cli_user_{SALT}', f'test_cli_p_role_{SALT}'),
            checker=lambda s: s == '')
        check_output(root_cli(
            'catalog-roles',
            'grant',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--principal-role',
            f'test_cli_p_role_{SALT}',
            f'test_cli_c_role_{SALT}'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'privileges',
            'catalog',
            'grant',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--catalog-role',
            f'test_cli_c_role_{SALT}',
            'TABLE_READ_DATA'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'privileges',
            'namespace',
            'grant',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--catalog-role',
            f'test_cli_c_role_{SALT}',
            '--namespace',
            f'a_{SALT}',
            'TABLE_WRITE_DATA'
        ), checker=lambda s: s == '')
        check_output(root_cli(
            'privileges',
            'namespace',
            'grant',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--catalog-role',
            f'test_cli_c_role_{SALT}',
            '--namespace',
            f'a_{SALT}',
            'TABLE_LIST'
        ), checker=lambda s: s == '')

        # List privileges:
        check_output(root_cli(
            'privileges',
            'list',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            '--catalog-role',
            f'test_cli_c_role_{SALT}'
        ), checker=lambda s: len(s.strip().split('\n')) == 3)

    finally:
        sys.path.pop(0)
    pass


def test_invalid_commands():
    sys.path.insert(0, CLI_PYTHONPATH)
    try:
        check_exception(root_cli('catalogs', 'create', 'test_catalog'), exception_str='--storage-type')
        check_exception(root_cli(
            'catalogs',
            'create',
            'test_catalog',
            '--storage-type',
            'not-real!'
        ), exception_str='--storage-type')
    finally:
        sys.path.pop(0)
