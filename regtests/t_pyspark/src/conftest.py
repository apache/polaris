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
from polaris.management import Catalog, AwsStorageConfigInfo, ApiClient, PolarisDefaultApi, Configuration, \
  CreateCatalogRequest, GrantCatalogRoleRequest, CatalogRole, ApiException, AddGrantRequest, CatalogGrant, \
  CatalogPrivilege, CreateCatalogRoleRequest


@pytest.fixture
def polaris_host():
  return os.getenv('POLARIS_HOST', 'localhost')


@pytest.fixture
def polaris_port():
  return int(os.getenv('POLARIS_PORT', '8181'))


@pytest.fixture
def polaris_url(polaris_host, polaris_port):
  return f"http://{polaris_host}:{polaris_port}/api/management/v1"


@pytest.fixture
def polaris_catalog_url(polaris_host, polaris_port):
  return f"http://{polaris_host}:{polaris_port}/api/catalog"

@pytest.fixture
def test_bucket():
  return os.getenv('AWS_STORAGE_BUCKET')

@pytest.fixture
def aws_role_arn():
  return os.getenv('AWS_ROLE_ARN')

@pytest.fixture
def catalog_client(polaris_catalog_url):
  """
  Create an iceberg catalog client with root credentials
  :param polaris_catalog_url:
  :param snowman:
  :return:
  """
  client = CatalogApiClient(
    Configuration(access_token=os.getenv('REGTEST_ROOT_BEARER_TOKEN'),
                  host=polaris_catalog_url))
  return IcebergCatalogAPI(client)


@pytest.fixture
def snowflake_catalog(root_client, catalog_client, test_bucket, aws_role_arn):
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


def create_catalog_role(api, catalog, role_name):
  catalog_role = CatalogRole(name=role_name)
  try:
    api.create_catalog_role(catalog_name=catalog.name,
                            create_catalog_role_request=CreateCatalogRoleRequest(catalog_role=catalog_role))
    return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)
  except ApiException as e:
    return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)
  else:
    raise e


def clear_namespace(catalog: str, catalog_client: IcebergCatalogAPI, namespace: List[str]):
  formatted_namespace = format_namespace(namespace)
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
  return codecs.decode("1F", "hex").decode("UTF-8").join(namespace)


@pytest.fixture
def root_client(polaris_host, polaris_url):
  client = ApiClient(Configuration(access_token=os.getenv('REGTEST_ROOT_BEARER_TOKEN'),
                                   host=polaris_url))
  api = PolarisDefaultApi(client)
  return api
