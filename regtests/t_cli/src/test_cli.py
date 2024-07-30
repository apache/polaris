import contextlib
import io
import json
import os
import random
import requests
import string
import subprocess
import sys
from typing import Callable

CLI_PYTHONPATH = f'{os.path.dirname(os.path.abspath(__file__))}/../../client/python'
ROLE_ARN = 'arn:aws:iam::123456789012:role/my-role'


def get_salt(length=8) -> str:
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def root_cli(*args):
    return cli('principal:root;realm:default-realm')(*args)


def cli(access_token):
    def cli_inner(*args) -> Callable[[], str]:
        def f() -> str:
            result = subprocess.run([
                'bash',
                f'{CLI_PYTHONPATH}/../../../polaris',
                '--access-token',
                access_token,
                '--host',
                'polaris',
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
    url = 'http://polaris:8181/api/catalog/v1/oauth/tokens'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'PRINCIPAL_ROLE:ALL'
    }

    response = requests.post(url, data=data)

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
            f'CATALOG_MANAGE_CONTENT'
        ), checker=lambda s: s == '')

        # User now has catalog access:
        check_output(cli(user_token)('catalogs', 'get', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: 's3://fake-location' in s)
        check_output(cli(user_token)(
            'namespaces',
            'create',
            '--catalog',
            f'test_cli_catalog_{SALT}',
            f'test_cli_namespace_{SALT}'
        ), checker=lambda s: s == '')
        check_output(cli(user_token)('namespaces', 'list', '--catalog', f'test_cli_catalog_{SALT}'),
                     checker=lambda s: f'test_cli_namespace_{SALT}' in s)
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
        ), checker=lambda s: f'a_{SALT}.b_{SALT}.c_{SALT}' in s)
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
            f'TABLE_READ_DATA'
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
            f'TABLE_WRITE_DATA'
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
            f'TABLE_LIST'
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
