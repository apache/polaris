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

import codecs
import os
from typing import List

import pytest

from polaris.catalog.api.iceberg_catalog_api import IcebergCatalogAPI
from polaris.catalog.api_client import ApiClient as CatalogApiClient
from polaris.catalog.api.iceberg_o_auth2_api import IcebergOAuth2API
from polaris.management import ApiClient as ManagementApiClient
from polaris.management import *


@pytest.fixture
def polaris_host():
  """
  Fixture that returns the host for the Polaris service.
  Defaults to 'localhost' if not provided via environment variables.
  """
  return os.getenv('POLARIS_HOST', 'localhost')


@pytest.fixture
def polaris_port():
  """
  Fixture that returns the port for the Polaris service.
  Defaults to 8181 if not provided via environment variables.
  """
  return int(os.getenv('POLARIS_PORT', '8181'))


@pytest.fixture
def polaris_url(polaris_host, polaris_port):
  """
  Fixture that returns the full URL for the Polaris management API.

  :param polaris_host: The host for the Polaris service.
  :param polaris_port: The port for the Polaris service.
  :return: A string URL for the Polaris management API.
  """
  return f"http://{polaris_host}:{polaris_port}/api/management/v1"


@pytest.fixture
def polaris_catalog_url(polaris_host, polaris_port):
  """
  Fixture that returns the full URL for the Polaris catalog API.

  :param polaris_host: The host for the Polaris service.
  :param polaris_port: The port for the Polaris service.
  :return: A string URL for the Polaris catalog API.
  """
  return f"http://{polaris_host}:{polaris_port}/api/catalog"


@pytest.fixture
def test_bucket():
  """
  Fixture that retrieves the AWS S3 bucket name from the environment variables.

  :return: The name of the AWS storage bucket.
  """
  return os.getenv('AWS_STORAGE_BUCKET')


@pytest.fixture
def aws_role_arn():
  """
  Fixture that retrieves the AWS role ARN from the environment variables.

  :return: The ARN for the AWS role.
  """
  return os.getenv('AWS_ROLE_ARN')


@pytest.fixture
def gcs_test_base():
  """
  Fixture that retrieves the Google Cloud Storage test base from the environment variables.

  :return: The base path for GCS test base.
  """
  return os.getenv('GCS_TEST_BASE')

@pytest.fixture
def azure_blob_test_base():
  """
  Fixture that retrieves the Azure Blob test base from the environment variables.

  :return: The base path for GCS test base.
  """
  return os.getenv('AZURE_BLOB_TEST_BASE')

@pytest.fixture
def azure_dfs_test_base():
  """
  Fixture that retrieves the Azure DFS test base from the environment variables.

  :return: The base path for GCS test base.
  """
  return os.getenv('AZURE_DFS_TEST_BASE')


@pytest.fixture
def catalog_client(polaris_catalog_url):
  """
  Fixture that creates and returns an IcebergCatalogAPI client configured with root credentials.

  :param polaris_catalog_url: The URL for the Polaris catalog API.
  :return: An IcebergCatalogAPI client instance.
  """
  client = CatalogApiClient(
    Configuration(access_token=os.getenv('REGTEST_ROOT_BEARER_TOKEN', 'principal:root;realm:default-realm'),
                  host=polaris_catalog_url))
  return IcebergCatalogAPI(client)


@pytest.fixture
def snowflake_catalog(root_client, catalog_client, test_bucket, aws_role_arn):
  """
  Fixture that creates a snowflake catalog, assigns roles, and performs setup for testing.

  :param root_client: The root client used to interact with Polaris APIs.
  :param catalog_client: The catalog client used to interact with the Iceberg catalog.
  :param test_bucket: The AWS S3 bucket for storage configuration.
  :param aws_role_arn: The AWS role ARN for the catalog's permissions.
  :return: The created catalog after setup.
  """
  storage_conf = AwsStorageConfigInfo(storage_type="S3",
                                      allowed_locations=[f"s3://{test_bucket}/polaris_test/"],
                                      role_arn=aws_role_arn)
  catalog_name = 'snowflake'
  catalog = Catalog(name=catalog_name, type='INTERNAL', properties={
    "default-base-location": f"s3://{test_bucket}/polaris_test/snowflake_catalog",
    "client.credentials-provider": "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider"
  },
                    storage_config_info=storage_conf)
  catalog.storage_config_info = storage_conf
  try:
    root_client.create_catalog(create_catalog_request=CreateCatalogRequest(catalog=catalog))
    resp = root_client.get_catalog(catalog_name=catalog.name)
    root_client.assign_catalog_role_to_principal_role(principal_role_name='service_admin',
                                                      catalog_name=catalog_name,
                                                      grant_catalog_role_request=GrantCatalogRoleRequest(
                                                        catalog_role=CatalogRole(name='catalog_admin')))
    writer_catalog_role = create_catalog_role(root_client, resp, 'admin_writer')
    root_client.add_grant_to_catalog_role(catalog_name, writer_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=catalog_name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.CATALOG_MANAGE_CONTENT)))
    root_client.assign_catalog_role_to_principal_role(principal_role_name='service_admin',
                                                      catalog_name=catalog_name,
                                                      grant_catalog_role_request=GrantCatalogRoleRequest(
                                                        catalog_role=writer_catalog_role))
    yield resp
  finally:
    namespaces = catalog_client.list_namespaces(catalog_name)
    for n in namespaces.namespaces:
      clear_namespace(catalog_name, catalog_client, n)
    catalog_roles = root_client.list_catalog_roles(catalog_name)
    for r in catalog_roles.roles:
      if r.name != 'catalog_admin':
        root_client.delete_catalog_role(catalog_name, r.name)
    root_client.delete_catalog(catalog_name=catalog_name)


@pytest.fixture
def root_client(polaris_host, polaris_url):
  """
  Fixture that creates and returns a root client for Polaris management API.

  :param polaris_host: The host for the Polaris service.
  :param polaris_url: The URL for the Polaris management API.
  :return: A PolarisDefaultApi client instance.
  """
  client = ApiClient(
    Configuration(access_token=os.getenv('REGTEST_ROOT_BEARER_TOKEN', 'principal:root;realm:default-realm'),
                  host=polaris_url))
  api = PolarisDefaultApi(client)
  return api


def clear_namespace(catalog: str, catalog_client: IcebergCatalogAPI, namespaces: List[str]):
  """
  Clears the specified namespaces in the catalog by dropping tables, views, and namespaces.

  :param catalog: The name of the catalog.
  :param catalog_client: The IcebergCatalogAPI client.
  :param namespaces: The namespace list to be cleared.
  """
  formatted_namespace = format_namespace(namespaces)
  tables = catalog_client.list_tables(catalog, formatted_namespace)
  for t in tables.identifiers:
    catalog_client.drop_table(catalog, format_namespace(t.namespace), t.name, purge_requested=True)
  views = catalog_client.list_views(catalog, formatted_namespace)
  for v in views.identifiers:
    catalog_client.drop_view(catalog, format_namespace(v.namespace), v.name)
  nested_namespaces = catalog_client.list_namespaces(catalog, parent=formatted_namespace)
  for n in nested_namespaces.namespaces:
    clear_namespace(catalog, catalog_client, n)
  catalog_client.drop_namespace(catalog, formatted_namespace)


def format_namespace(namespace):
  """
  Formats the namespace by joining its components.

  :param namespace: The list of namespace components.
  :return: The formatted namespace string.
  """
  return codecs.decode("1F", "hex").decode("UTF-8").join(namespace)


def create_principal(polaris_url, polaris_catalog_url, api, principal_name):
  """
  Creates a principal and returns its rotated credentials.

  :param polaris_url: The URL for the Polaris management API.
  :param polaris_catalog_url: The URL for the Polaris catalog API.
  :param api: The PolarisDefaultApi client instance.
  :param principal_name: The name of the principal to create.
  :return: The rotated credentials for the created principal.
  """
  principal = Principal(name=principal_name, type="SERVICE")
  try:
    principal_result = api.create_principal(CreatePrincipalRequest(principal=principal))

    token_client = CatalogApiClient(Configuration(username=principal_result.principal.client_id,
                                                  password=principal_result.credentials.client_secret,
                                                  host=polaris_catalog_url))
    oauth_api = IcebergOAuth2API(token_client)
    token = oauth_api.get_token(scope='PRINCIPAL_ROLE:ALL', client_id=principal_result.principal.client_id,
                                client_secret=principal_result.credentials.client_secret,
                                grant_type='client_credentials',
                                _headers={'realm': 'default-realm'})
    rotate_client = ManagementApiClient(Configuration(access_token=token.access_token,
                                                      host=polaris_url))
    rotate_api = PolarisDefaultApi(rotate_client)

    rotate_credentials = rotate_api.rotate_credentials(principal_name=principal_name)
    return rotate_credentials
  except ApiException as e:
      raise e


def create_principal_role(api, role_name):
  """
  Creates a principal role and returns the created role.

  :param api: The PolarisDefaultApi client instance.
  :param role_name: The name of the principal role to create.
  :return: The created principal role.
  """
  principal_role = PrincipalRole(name=role_name)
  try:
    api.create_principal_role(CreatePrincipalRoleRequest(principal_role=principal_role))
    return api.get_principal_role(principal_role_name=role_name)
  except ApiException as e:
    raise e


def create_catalog_role(api, catalog, role_name):
  """
  Creates a catalog role and returns the created catalog role.

  :param api: The PolarisDefaultApi client instance.
  :param catalog: The catalog to which the role belongs.
  :param role_name: The name of the catalog role to create.
  :return: The created catalog role.
  """
  catalog_role = CatalogRole(name=role_name)
  try:
    api.create_catalog_role(catalog_name=catalog.name,
                            create_catalog_role_request=CreateCatalogRoleRequest(catalog_role=catalog_role))
    return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)
  except ApiException as e:
    raise e
