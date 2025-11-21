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
import uuid
from typing import List

import pytest

from apache_polaris.sdk.catalog.api.iceberg_catalog_api import IcebergCatalogAPI
from apache_polaris.sdk.catalog.api_client import ApiClient as CatalogApiClient
from apache_polaris.sdk.management import Catalog, AwsStorageConfigInfo, ApiClient, PolarisDefaultApi, Configuration, \
  CreateCatalogRequest, GrantCatalogRoleRequest, CatalogRole, ApiException, AddGrantRequest, CatalogGrant, \
  CatalogPrivilege, CreateCatalogRoleRequest, CatalogProperties


@pytest.fixture
def polaris_host():
  return os.getenv('POLARIS_HOST', 'localhost')


@pytest.fixture
def polaris_port():
  return int(os.getenv('POLARIS_PORT', '8181'))

@pytest.fixture
def polaris_path_prefix():
  """
  Used to provide a path prefix between the port number and the standard polaris endpoint paths.
  No leading or trailing /
  :return:
  """
  return os.getenv('POLARIS_PATH_PREFIX', '')

@pytest.fixture
def polaris_url_scheme():
  """
  The URL Schema - either http or https - no : or trailing /
  :return:
  """
  return os.getenv('POLARIS_URL_SCHEME', 'http')

@pytest.fixture
def polaris_url(polaris_url_scheme, polaris_host, polaris_port, polaris_path_prefix):
  polaris_path_prefix = polaris_path_prefix if len(polaris_path_prefix) == 0 else '/' + polaris_path_prefix
  return f"{polaris_url_scheme}://{polaris_host}:{polaris_port}{polaris_path_prefix}/api/management/v1"


@pytest.fixture
def polaris_catalog_url(polaris_url_scheme, polaris_host, polaris_port, polaris_path_prefix):
  polaris_path_prefix = polaris_path_prefix if len(polaris_path_prefix) == 0 else '/' + polaris_path_prefix
  return f"{polaris_url_scheme}://{polaris_host}:{polaris_port}{polaris_path_prefix}/api/catalog"

@pytest.fixture
def test_bucket():
  return os.getenv('AWS_STORAGE_BUCKET')

@pytest.fixture
def aws_role_arn():
  return os.getenv('AWS_ROLE_ARN')


@pytest.fixture
def aws_bucket_base_location_prefix():
  """
  :return: Base location prefix for tests, excluding leading and trailing '/'
    Provides a default if null or empty
  """
  default_val = 'polaris_test'
  bucket_prefix = os.getenv('AWS_BUCKET_BASE_LOCATION_PREFIX', default_val)
  # Add random string to prefix to prevent base location overlaps
  return f"{default_val if bucket_prefix == '' else bucket_prefix}_{str(uuid.uuid4())[:5]}"

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
def snowflake_catalog(root_client, catalog_client, test_bucket, aws_role_arn, aws_bucket_base_location_prefix):
  storage_conf = AwsStorageConfigInfo(storage_type="S3",
                                      allowed_locations=[f"s3://{test_bucket}/{aws_bucket_base_location_prefix}/"],
                                      role_arn=aws_role_arn)
  catalog_name = f'snowflake_{str(uuid.uuid4())[-10:]}'
  catalog = Catalog(name=catalog_name, type='INTERNAL', properties=CatalogProperties.from_dict({
    "default-base-location": f"s3://{test_bucket}/{aws_bucket_base_location_prefix}/snowflake_catalog",
    "client.credentials-provider": "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider",
    "polaris.config.drop-with-purge.enabled": "true"
  }),
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
  except ApiException:
    return api.get_catalog_role(catalog_name=catalog.name, catalog_role_name=role_name)
  else:
    raise e


def clear_namespace(catalog: str, catalog_client: IcebergCatalogAPI, namespace: List[str]):
  formatted_namespace = format_namespace(namespace)
  tables = catalog_client.list_tables(prefix=catalog, namespace=formatted_namespace)
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

# Helper function to create catalog with specific storage configuration
def _create_catalog_with_storage(root_client, catalog_client, catalog_name, storage_config_info, base_location):
  """
  Internal helper to create a catalog with specific storage configuration.
  
  Args:
    root_client: Management API client
    catalog_client: Catalog API client  
    catalog_name: Name for the catalog
    storage_config_info: Storage configuration (S3 or FILE)
    base_location: Base location for the catalog
  """
  from apache_polaris.sdk.management import AwsStorageConfigInfo
  
  # Build properties dict
  catalog_properties = {
    "default-base-location": base_location,
    "polaris.config.drop-with-purge.enabled": "true"
  }
  
  # Add AWS-specific properties if using S3 storage
  if isinstance(storage_config_info, AwsStorageConfigInfo):
    catalog_properties["client.credentials-provider"] = "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider"
  
  catalog = Catalog(name=catalog_name, type='INTERNAL', 
                    properties=CatalogProperties.from_dict(catalog_properties),
                    storage_config_info=storage_config_info)
  
  try:
    root_client.create_catalog(create_catalog_request=CreateCatalogRequest(catalog=catalog))
    resp = root_client.get_catalog(catalog_name=catalog.name)
    
    # Set up basic catalog role with admin privileges
    root_client.assign_catalog_role_to_principal_role(
      principal_role_name='service_admin',
      catalog_name=catalog_name,
      grant_catalog_role_request=GrantCatalogRoleRequest(
        catalog_role=CatalogRole(name='catalog_admin')
      )
    )
    
    writer_catalog_role = create_catalog_role(root_client, resp, 'admin_writer')
    root_client.add_grant_to_catalog_role(
      catalog_name, writer_catalog_role.name,
      AddGrantRequest(grant=CatalogGrant(
        catalog_name=catalog_name,
        type='catalog',
        privilege=CatalogPrivilege.CATALOG_MANAGE_CONTENT
      ))
    )
    
    root_client.assign_catalog_role_to_principal_role(
      principal_role_name='service_admin',
      catalog_name=catalog_name,
      grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=writer_catalog_role)
    )
    
    yield resp
  finally:
    # Cleanup
    namespaces = catalog_client.list_namespaces(catalog_name)
    for n in namespaces.namespaces:
      clear_namespace(catalog_name, catalog_client, n)
    catalog_roles = root_client.list_catalog_roles(catalog_name)
    for r in catalog_roles.roles:
      if r.name != 'catalog_admin':
        root_client.delete_catalog_role(catalog_name, r.name)
    root_client.delete_catalog(catalog_name=catalog_name)


@pytest.fixture
def file_catalog(root_client, catalog_client):
  """
  Catalog that always uses FILE storage for local testing.
  This fixture runs in any environment without external dependencies.
  """
  from apache_polaris.sdk.management import FileStorageConfigInfo
  
  catalog_name = f'file_catalog_{str(uuid.uuid4())[-10:]}'
  storage_config = FileStorageConfigInfo(storage_type="FILE", allowed_locations=["file:///tmp"])
  base_location = "file:///tmp/polaris"
  
  yield from _create_catalog_with_storage(
    root_client, catalog_client, catalog_name, storage_config, base_location
  )


@pytest.fixture  
def s3_catalog(root_client, catalog_client, test_bucket, aws_role_arn, aws_bucket_base_location_prefix):
    """
    Catalog that always uses S3 storage for AWS testing.
    Tests using this fixture should include @pytest.mark.skipif for AWS_TEST_ENABLED.
    """
    from apache_polaris.sdk.management import AwsStorageConfigInfo
    
    catalog_name = f's3_catalog_{str(uuid.uuid4())[-10:]}'
    storage_config = AwsStorageConfigInfo(
      storage_type="S3",
      allowed_locations=[f"s3://{test_bucket}/{aws_bucket_base_location_prefix}/"],
      role_arn=aws_role_arn
    )
    base_location = f"s3://{test_bucket}/{aws_bucket_base_location_prefix}/s3_catalog"
    
    yield from _create_catalog_with_storage(
      root_client, catalog_client, catalog_name, storage_config, base_location
    )


