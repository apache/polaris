#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import codecs
import os
from typing import List

import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, BooleanType

from polaris.catalog import OAuthTokenResponse
from polaris.catalog.api.iceberg_catalog_api import IcebergCatalogAPI
from polaris.catalog.api.iceberg_o_auth2_api import IcebergOAuth2API
from polaris.catalog.api_client import ApiClient as CatalogApiClient
from polaris.catalog.api_client import Configuration as CatalogApiClientConfiguration
from polaris.management import (
    Catalog,
    ApiClient,
    PolarisDefaultApi,
    Configuration,
    CreateCatalogRequest,
    GrantCatalogRoleRequest,
    CatalogRole,
    ApiException,
    AddGrantRequest,
    CatalogGrant,
    CatalogPrivilege,
    CreateCatalogRoleRequest,
    FileStorageConfigInfo,
    Principal,
    CreatePrincipalRequest,
    PrincipalRole,
    CreatePrincipalRoleRequest,
    GrantPrincipalRoleRequest,
    PrincipalWithCredentials,
)


@pytest.fixture
def polaris_host() -> str:
    return os.getenv("POLARIS_HOST", "localhost")


@pytest.fixture
def polaris_port() -> int:
    return int(os.getenv("POLARIS_PORT", "8181"))


@pytest.fixture
def polaris_path_prefix() -> str:
    """
    Used to provide a path prefix between the port number and the standard polaris endpoint paths.
    No leading or trailing /
    :return:
    """
    return os.getenv("POLARIS_PATH_PREFIX", "")


@pytest.fixture
def polaris_url_scheme() -> str:
    """
    The URL Schema - either http or https - no : or trailing /
    :return:
    """
    return os.getenv("POLARIS_URL_SCHEME", "http")


@pytest.fixture
def polaris_root_credential() -> str:
    return os.getenv("POLARIS_ROOT_CREDENTIAL", "root:s3cr3t")


@pytest.fixture
def polaris_url(
    polaris_url_scheme: str,
    polaris_host: str,
    polaris_port: int,
    polaris_path_prefix: str,
) -> str:
    polaris_path_prefix = (
        polaris_path_prefix
        if len(polaris_path_prefix) == 0
        else "/" + polaris_path_prefix
    )
    return f"{polaris_url_scheme}://{polaris_host}:{polaris_port}{polaris_path_prefix}/api/management/v1"


@pytest.fixture
def polaris_catalog_url(
    polaris_url_scheme: str,
    polaris_host: str,
    polaris_port: int,
    polaris_path_prefix: str,
) -> str:
    polaris_path_prefix = (
        polaris_path_prefix
        if len(polaris_path_prefix) == 0
        else "/" + polaris_path_prefix
    )
    return f"{polaris_url_scheme}://{polaris_host}:{polaris_port}{polaris_path_prefix}/api/catalog"


@pytest.fixture
def root_token(
    polaris_catalog_url: str, polaris_root_credential: str
) -> OAuthTokenResponse:
    client_id, client_secret = polaris_root_credential.split(":")
    client = CatalogApiClient(
        CatalogApiClientConfiguration(
            username=client_id, password=client_secret, host=polaris_catalog_url
        )
    )
    oauth_api = IcebergOAuth2API(client)
    token = oauth_api.get_token(
        scope="PRINCIPAL_ROLE:ALL",
        client_id=client_id,
        client_secret=client_secret,
        grant_type="client_credentials",
        _headers={"realm": "default-realm"},
    )
    return token


@pytest.fixture
def management_client(
    polaris_url: str, root_token: OAuthTokenResponse
) -> PolarisDefaultApi:
    client = ApiClient(
        Configuration(access_token=root_token.access_token, host=polaris_url)
    )
    root_client = PolarisDefaultApi(client)
    return root_client


@pytest.fixture
def test_principal(management_client: PolarisDefaultApi) -> PrincipalWithCredentials:
    yield create_principal(management_client, "test_principal")
    management_client.delete_principal(principal_name="test_principal")


@pytest.fixture
def test_principal_role(management_client: PolarisDefaultApi) -> PrincipalRole:
    yield create_principal_role(management_client, "test_principal_role")
    management_client.delete_principal_role(principal_role_name="test_principal_role")


@pytest.fixture
def test_principal_token(
    polaris_catalog_url: str, test_principal: PrincipalWithCredentials
) -> OAuthTokenResponse:
    client = CatalogApiClient(
        CatalogApiClientConfiguration(
            host=polaris_catalog_url,
            username=test_principal.principal.client_id,
            password=test_principal.credentials.client_secret,
        )
    )
    oauth_api = IcebergOAuth2API(client)
    return oauth_api.get_token(
        scope="PRINCIPAL_ROLE:ALL",
        client_id=test_principal.principal.client_id,
        client_secret=test_principal.credentials.client_secret.get_secret_value(),
        grant_type="client_credentials",
        _headers={"realm": "POLARIS"},
    )


@pytest.fixture
def test_catalog_client(
    polaris_catalog_url: str, test_principal_token: OAuthTokenResponse
) -> IcebergCatalogAPI:
    """
    Creates a catalog client for the given test catalog.
    :param polaris_catalog_url: The URL of the Polaris catalog service.
    :param test_principal: The principal to use for authentication.
    :return: An IcebergCatalogAPI client for the test catalog.
    """

    return IcebergCatalogAPI(
        CatalogApiClient(
            Configuration(
                access_token=test_principal_token.access_token, host=polaris_catalog_url
            )
        )
    )


@pytest.fixture
def test_catalog(
    management_client: PolarisDefaultApi,
    test_principal: PrincipalWithCredentials,
    test_principal_role: PrincipalRole,
    test_catalog_client: IcebergCatalogAPI,
) -> Catalog:
    storage_conf = FileStorageConfigInfo(
        storage_type="FILE", allowed_locations=["file:///tmp"]
    )
    catalog_name = "polaris_test_catalog"
    catalog = Catalog(
        name=catalog_name,
        type="INTERNAL",
        properties={"default-base-location": "file:///tmp/polaris/"},
        storage_config_info=storage_conf,
    )
    catalog.storage_config_info = storage_conf
    try:
        management_client.create_catalog(
            create_catalog_request=CreateCatalogRequest(catalog=catalog)
        )
        resp = management_client.get_catalog(catalog_name=catalog.name)
        catalog_role = create_catalog_role(management_client, resp, "manage_catalog")
        management_client.assign_catalog_role_to_principal_role(
            principal_role_name=test_principal_role.name,
            catalog_name=resp.name,
            grant_catalog_role_request=GrantCatalogRoleRequest(
                catalog_role=catalog_role
            ),
        )
        management_client.add_grant_to_catalog_role(
            resp.name,
            catalog_role.name,
            AddGrantRequest(
                grant=CatalogGrant(
                    type="catalog", privilege=CatalogPrivilege.CATALOG_MANAGE_CONTENT
                )
            ),
        )
        management_client.assign_principal_role(
            test_principal.principal.name,
            grant_principal_role_request=GrantPrincipalRoleRequest(
                principal_role=test_principal_role
            ),
        )
        yield resp
    finally:
        namespaces = test_catalog_client.list_namespaces(catalog_name)
        for n in namespaces.namespaces:
            clear_namespace(catalog_name, test_catalog_client, n)
        catalog_roles = management_client.list_catalog_roles(catalog_name)
        for r in catalog_roles.roles:
            if r.name != "catalog_admin":
                management_client.delete_catalog_role(catalog_name, r.name)
        management_client.delete_catalog(catalog_name=catalog_name)


@pytest.fixture
def test_pyiceberg_catalog(
    test_principal: PrincipalWithCredentials,
    test_catalog: Catalog,
    polaris_catalog_url: str,
) -> PyIcebergCatalog:
    return load_catalog(
        name="rest",
        **{
            "type": "rest",
            "uri": polaris_catalog_url,
            "warehouse": test_catalog.name,
            "credential": f"{test_principal.principal.client_id}:{test_principal.credentials.client_secret.get_secret_value()}",
            "scope": "PRINCIPAL_ROLE:ALL",
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "client.region": "us-west-2",
        },
    )


@pytest.fixture(scope="session")
def test_table_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
        NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
        schema_id=1,
        identifier_field_ids=[2],
    )


# Creates a principal with the given name
def create_principal(
    api: PolarisDefaultApi, principal_name: str
) -> PrincipalWithCredentials:
    principal = Principal(name=principal_name, type="SERVICE")
    try:
        principal_result = api.create_principal(
            CreatePrincipalRequest(principal=principal)
        )
        return principal_result
    except ApiException as e:
        if e.status == 409:
            return api.rotate_credentials(principal_name=principal_name)
        else:
            raise e


# Create a catalog role with the given name
def create_catalog_role(
    api: PolarisDefaultApi, catalog: Catalog, role_name: str
) -> CatalogRole:
    catalog_role = CatalogRole(name=role_name)
    api.create_catalog_role(
        catalog_name=catalog.name,
        create_catalog_role_request=CreateCatalogRoleRequest(catalog_role=catalog_role),
    )
    return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)


# Create a principal role with the given name
def create_principal_role(api: PolarisDefaultApi, role_name: str) -> PrincipalRole:
    principal_role = PrincipalRole(name=role_name)
    try:
        api.create_principal_role(
            CreatePrincipalRoleRequest(principal_role=principal_role)
        )
        return api.get_principal_role(principal_role_name=role_name)
    except ApiException:
        return api.get_principal_role(principal_role_name=role_name)


def clear_namespace(
    catalog: str, catalog_client: IcebergCatalogAPI, namespace: List[str]
) -> None:
    formatted_namespace = format_namespace(namespace)
    tables = catalog_client.list_tables(prefix=catalog, namespace=formatted_namespace)
    for t in tables.identifiers:
        catalog_client.drop_table(
            catalog, format_namespace(t.namespace), t.name, purge_requested=True
        )
    views = catalog_client.list_views(catalog, formatted_namespace)
    for v in views.identifiers:
        catalog_client.drop_view(catalog, format_namespace(v.namespace), v.name)
    nested_namespaces = catalog_client.list_namespaces(
        catalog, parent=formatted_namespace
    )
    for n in nested_namespaces.namespaces:
        clear_namespace(catalog, catalog_client, n)
    catalog_client.drop_namespace(catalog, formatted_namespace)


def format_namespace(namespace: List[str]) -> str:
    return codecs.decode("1F", "hex").decode("UTF-8").join(namespace)
