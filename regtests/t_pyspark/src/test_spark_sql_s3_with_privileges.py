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
import time
import uuid
from urllib.parse import unquote

import boto3
import botocore
import pytest
from py4j.protocol import Py4JJavaError

from botocore.exceptions import ClientError

from iceberg_spark import IcebergSparkSession
from polaris.catalog import CreateNamespaceRequest, CreateTableRequest, ModelSchema, StructField
from polaris.catalog.api.iceberg_catalog_api import IcebergCatalogAPI
from polaris.catalog.api.iceberg_o_auth2_api import IcebergOAuth2API
from polaris.catalog.api_client import ApiClient as CatalogApiClient
from polaris.catalog.configuration import Configuration
from polaris.management import ApiClient as ManagementApiClient
from polaris.management import PolarisDefaultApi, Principal, PrincipalRole, CatalogRole, \
  CatalogGrant, CatalogPrivilege, ApiException, CreateCatalogRoleRequest, CreatePrincipalRoleRequest, \
  CreatePrincipalRequest, AddGrantRequest, GrantCatalogRoleRequest, GrantPrincipalRoleRequest, UpdateCatalogRequest
from polaris.management.exceptions import ForbiddenException


@pytest.fixture
def snowman(polaris_url, polaris_catalog_url, root_client, snowflake_catalog):
  """
  create the snowman principal with full table/namespace privileges
  :param root_client:
  :param snowflake_catalog:
  :return:
  """
  snowman_name = "snowman"
  table_writer_rolename = "table_writer"
  snowflake_writer_rolename = "snowflake_writer"
  try:
    snowman = create_principal(polaris_url, polaris_catalog_url, root_client, snowman_name)
    writer_principal_role = create_principal_role(root_client, table_writer_rolename)
    writer_catalog_role = create_catalog_role(root_client, snowflake_catalog, snowflake_writer_rolename)
    root_client.assign_catalog_role_to_principal_role(principal_role_name=writer_principal_role.name,
                                                      catalog_name=snowflake_catalog.name,
                                                      grant_catalog_role_request=GrantCatalogRoleRequest(
                                                        catalog_role=writer_catalog_role))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, writer_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.TABLE_FULL_METADATA)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, writer_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.VIEW_FULL_METADATA)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, writer_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.TABLE_WRITE_DATA)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, writer_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.NAMESPACE_FULL_METADATA)))

    root_client.assign_principal_role(snowman.principal.name,
                                      grant_principal_role_request=GrantPrincipalRoleRequest(
                                        principal_role=writer_principal_role))
    yield snowman
  finally:
    root_client.delete_principal(snowman_name)
    root_client.delete_principal_role(principal_role_name=table_writer_rolename)
    root_client.delete_catalog_role(catalog_role_name=snowflake_writer_rolename, catalog_name=snowflake_catalog.name)


@pytest.fixture
def reader(polaris_url, polaris_catalog_url, root_client, snowflake_catalog):
  """
  create the test_reader principal with table/namespace list and read privileges

  :param root_client:
  :param snowflake_catalog:
  :return:
  """
  reader_principal_name = 'test_reader'
  reader_principal_role_name = "table_reader"
  reader_catalog_role_name = 'snowflake_reader'
  try:
    reader = create_principal(polaris_url, polaris_catalog_url, root_client, reader_principal_name)
    reader_principal_role = create_principal_role(root_client, reader_principal_role_name)
    reader_catalog_role = create_catalog_role(root_client, snowflake_catalog, reader_catalog_role_name)

    root_client.assign_catalog_role_to_principal_role(principal_role_name=reader_principal_role.name,
                                                      catalog_name=snowflake_catalog.name,
                                                      grant_catalog_role_request=GrantCatalogRoleRequest(
                                                        catalog_role=reader_catalog_role))
    root_client.assign_principal_role(reader.principal.name,
                                      grant_principal_role_request=GrantPrincipalRoleRequest(
                                        principal_role=reader_principal_role))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, reader_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.TABLE_READ_DATA)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, reader_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.TABLE_LIST)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, reader_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.TABLE_READ_PROPERTIES)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, reader_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.NAMESPACE_LIST)))
    root_client.add_grant_to_catalog_role(snowflake_catalog.name, reader_catalog_role.name,
                                          AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                             type='catalog',
                                                                             privilege=CatalogPrivilege.NAMESPACE_READ_PROPERTIES)))
    yield reader
  finally:
    root_client.delete_principal(reader_principal_name)
    root_client.delete_principal_role(principal_role_name=reader_principal_role_name)
    root_client.delete_catalog_role(catalog_role_name=reader_catalog_role_name, catalog_name=snowflake_catalog.name)


@pytest.fixture
def snowman_catalog_client(polaris_catalog_url, snowman):
  """
  Create an iceberg catalog client with snowman credentials
  :param polaris_catalog_url:
  :param snowman:
  :return:
  """
  client = CatalogApiClient(Configuration(username=snowman.principal.client_id,
                                          password=snowman.credentials.client_secret,
                                          host=polaris_catalog_url))
  oauth_api = IcebergOAuth2API(client)
  token = oauth_api.get_token(scope='PRINCIPAL_ROLE:ALL', client_id=snowman.principal.client_id,
                              client_secret=snowman.credentials.client_secret,
                              grant_type='client_credentials',
                              _headers={'realm': 'default-realm'})

  return IcebergCatalogAPI(CatalogApiClient(Configuration(access_token=token.access_token,
                                                          host=polaris_catalog_url)))

@pytest.fixture
def creator_catalog_client(polaris_catalog_url, creator):
    """
    Create an iceberg catalog client with TABLE_CREATE credentials
    :param polaris_catalog_url:
    :param creator:
    :return:
    """
    client = CatalogApiClient(Configuration(username=creator.principal.client_id,
                                            password=creator.credentials.client_secret,
                                            host=polaris_catalog_url))
    oauth_api = IcebergOAuth2API(client)
    token = oauth_api.get_token(scope='PRINCIPAL_ROLE:ALL', client_id=creator.principal.client_id,
                                client_secret=creator.credentials.client_secret,
                                grant_type='client_credentials',
                                _headers={'realm': 'default-realm'})

    return IcebergCatalogAPI(CatalogApiClient(Configuration(access_token=token.access_token,
                                                            host=polaris_catalog_url)))


@pytest.fixture
def creator(polaris_url, polaris_catalog_url, root_client, snowflake_catalog):
    """
    create the creator principal with only TABLE_CREATE privileges
    :param root_client:
    :param snowflake_catalog:
    :return:
    """
    creator_name = "creator"
    principal_role = "creator_principal_role"
    catalog_role = "creator_catalog_role"
    try:
        creator = create_principal(polaris_url, polaris_catalog_url, root_client, creator_name)
        creator_principal_role = create_principal_role(root_client, principal_role)
        creator_catalog_role = create_catalog_role(root_client, snowflake_catalog, catalog_role)

        root_client.assign_catalog_role_to_principal_role(principal_role_name=creator_principal_role.name,
                                                          catalog_name=snowflake_catalog.name,
                                                          grant_catalog_role_request=GrantCatalogRoleRequest(
                                                              catalog_role=creator_catalog_role))
        root_client.add_grant_to_catalog_role(snowflake_catalog.name, creator_catalog_role.name,
                                              AddGrantRequest(grant=CatalogGrant(catalog_name=snowflake_catalog.name,
                                                                                 type='catalog',
                                                                                 privilege=CatalogPrivilege.TABLE_CREATE)))
        root_client.assign_principal_role(creator.principal.name,
                                          grant_principal_role_request=GrantPrincipalRoleRequest(
                                              principal_role=creator_principal_role))
        yield creator
    finally:
        root_client.delete_principal(creator_name)
        root_client.delete_principal_role(principal_role_name=principal_role)
        root_client.delete_catalog_role(catalog_role_name=catalog_role, catalog_name=snowflake_catalog.name)


@pytest.fixture
def reader_catalog_client(polaris_catalog_url, reader):
  """
  Create an iceberg catalog client with test_reader credentials
  :param polaris_catalog_url:
  :param reader:
  :return:
  """
  client = CatalogApiClient(Configuration(username=reader.principal.client_id,
                                          password=reader.credentials.client_secret,
                                          host=polaris_catalog_url))
  oauth_api = IcebergOAuth2API(client)
  token = oauth_api.get_token(scope='PRINCIPAL_ROLE:ALL', client_id=reader.principal.client_id,
                              client_secret=reader.credentials.client_secret,
                              grant_type='client_credentials',
                              _headers={'realm': 'default-realm'})

  return IcebergCatalogAPI(CatalogApiClient(Configuration(access_token=token.access_token,
                                                          host=polaris_catalog_url)))


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials(root_client, snowflake_catalog, polaris_catalog_url, snowman, reader):
  """
  Basic spark test - using snowman, create namespaces and a table. Insert into the table and read records back.

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    spark.sql('CREATE TABLE iceberg_table (col1 int, col2 string)')
    spark.sql('SHOW TABLES')
    spark.sql("""INSERT INTO iceberg_table VALUES 
        (10, 'mystring'),
        (20, 'anotherstring'),
        (30, null)
        """)
    count = spark.sql("SELECT * FROM iceberg_table").count()
    assert count == 3

  # switch users to the reader. we can query, show namespaces, but we can't insert
  with IcebergSparkSession(credentials=f'{reader.principal.client_id}:{reader.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    count = spark.sql("SELECT * FROM iceberg_table").count()
    assert count == 3
    try:
      spark.sql("""INSERT INTO iceberg_table VALUES 
            (10, 'mystring'),
            (20, 'anotherstring'),
            (30, null)
            """)
      pytest.fail("Expected exception when trying to write without permission")
    except:
      print("Exception caught attempting to write without permission")

  # switch back to delete stuff
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('USE db1.schema')
    spark.sql('DROP TABLE iceberg_table')
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('DROP NAMESPACE db1.schema')
    spark.sql('DROP NAMESPACE db1')


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_cannot_create_table_outside_of_namespace_dir(root_client, snowflake_catalog, polaris_catalog_url, snowman, reader):
  """
  Basic spark test - using snowman, create a namespace and try to create a table outside of the namespace. This should fail

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_location = snowflake_catalog.properties.default_base_location + '/db1/outside_schema/table_outside_namespace'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    try:
      spark.sql(f"CREATE TABLE iceberg_table_outside_namespace (col1 int, col2 string) LOCATION '{table_location}'")
      pytest.fail("Expected to fail when creating table outside of namespace directory")
    except Py4JJavaError as e:
      assert "is not in the list of allowed locations" in e.java_exception.getMessage()


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_creates_table_in_custom_namespace_dir(root_client, snowflake_catalog, polaris_catalog_url, snowman, reader):
  """
  Basic spark test - using snowman, create a namespace and try to create a table outside of the namespace. This should fail

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    namespace_location = snowflake_catalog.properties.default_base_location + '/db1/custom_location'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql(f"CREATE NAMESPACE db1.schema LOCATION '{namespace_location}'")
    spark.sql('USE db1.schema')
    spark.sql(f"CREATE TABLE table_in_custom_namespace_location (col1 int, col2 string)")
    assert spark.sql("SELECT * FROM table_in_custom_namespace_location").count() == 0
    # check the metadata and assert the custom namespace location is used
    entries = spark.sql(f"SELECT file FROM db1.schema.table_in_custom_namespace_location.metadata_log_entries").collect()
    assert namespace_location in entries[0][0]


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_can_create_table_in_custom_allowed_dir(root_client, snowflake_catalog, polaris_catalog_url, snowman, reader):
  """
  Basic spark test - using snowman, create a namespace and try to create a table outside of the namespace. This should fail

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_location = snowflake_catalog.properties.default_base_location + '/db1/custom_schema_location/table_outside_namespace'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql(f"CREATE NAMESPACE db1.schema LOCATION '{snowflake_catalog.properties.default_base_location}/db1/custom_schema_location'")
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    # this is supported because it is inside of the custom namespace location
    spark.sql(f"CREATE TABLE iceberg_table_outside_namespace (col1 int, col2 string) LOCATION '{table_location}'")


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_cannot_create_view_overlapping_table(root_client, snowflake_catalog, polaris_catalog_url, snowman, reader):
  """
  Basic spark test - using snowman, create a namespace and try to create a table outside of the namespace. This should fail

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_location = snowflake_catalog.properties.default_base_location + '/db1/schema/table_dir'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql(f"CREATE NAMESPACE db1.schema LOCATION '{snowflake_catalog.properties.default_base_location}/db1/schema'")
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    spark.sql(f"CREATE TABLE my_iceberg_table (col1 int, col2 string) LOCATION '{table_location}'")
    try:
      spark.sql(f"CREATE VIEW disallowed_view (int, string) TBLPROPERTIES ('location'= '{table_location}') AS SELECT * FROM my_iceberg_table")
      pytest.fail("Expected to fail when creating table outside of namespace directory")
    except Py4JJavaError as e:
      assert "conflicts with existing table or namespace at location" in e.java_exception.getMessage()


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials_can_delete_after_purge(root_client, snowflake_catalog, polaris_catalog_url, snowman,
                                                  snowman_catalog_client, test_bucket):
  """
  Using snowman, create namespaces and a table. Insert into the table in multiple operations and update existing records
  to generate multiple metadata.json files and manfiests. Drop the table with purge=true. Poll S3 and validate all of
  the files are deleted.

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_name = f'iceberg_test_table_{str(uuid.uuid4())[-10:]}'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    spark.sql(f'CREATE TABLE {table_name} (col1 int, col2 string)')
    spark.sql('SHOW TABLES')

    # several inserts and an update, which should cause earlier files to show up as deleted in the later manifests
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (10, 'mystring'),
        (20, 'anotherstring'),
        (30, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (40, 'mystring'),
        (50, 'anotherstring'),
        (60, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (70, 'mystring'),
        (80, 'anotherstring'),
        (90, null)
        """)
    spark.sql(f"UPDATE {table_name} SET col2='changed string' WHERE col1 BETWEEN 20 AND 50")
    count = spark.sql(f"SELECT * FROM {table_name}").count()

    assert count == 9

    # fetch aws credentials to examine the metadata files
    response = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'), table_name,
                                                 "true")
    assert response.config is not None
    assert 's3.access-key-id' in response.config
    assert 's3.secret-access-key' in response.config
    assert 's3.session-token' in response.config

    s3 = boto3.client('s3',
                      aws_access_key_id=response.config['s3.access-key-id'],
                      aws_secret_access_key=response.config['s3.secret-access-key'],
                      aws_session_token=response.config['s3.session-token'])

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=f'polaris_test/snowflake_catalog/db1/schema/{table_name}/data/')
    assert objects is not None
    assert 'Contents' in objects
    assert len(objects['Contents']) >= 4  # idk, it varies - at least one file for each inser and one for the update
    print(f"Found {len(objects['Contents'])} data files in S3 before drop")

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=f'polaris_test/snowflake_catalog/db1/schema/{table_name}/metadata/')
    assert objects is not None
    assert 'Contents' in objects
    assert len(objects['Contents']) == 15  # 5 metadata.json files, 4 manifest lists, and 6 manifests
    print(f"Found {len(objects['Contents'])} metadata files in S3 before drop")

    # use the api client to ensure the purge flag is set to true
    snowman_catalog_client.drop_table(snowflake_catalog.name,
                                      codecs.decode("1F", "hex").decode("UTF-8").join(['db1', 'schema']), table_name,
                                      purge_requested=True)
    spark.sql('DROP NAMESPACE db1.schema')
    spark.sql('DROP NAMESPACE db1')
    print("Dropped table with purge - waiting for files to be deleted")
    attempts = 0

    # watch the data directory. metadata will be deleted first, so if data directory is clear, we can expect
    # metadatat diretory to be clear also
    while 'Contents' in objects and len(objects['Contents']) > 0 and attempts < 60:
      time.sleep(1)  # seconds, not milliseconds ;)
      objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                                Prefix=f'polaris_test/snowflake_catalog/db1/schema/{table_name}/data/')
      attempts = attempts + 1

    if 'Contents' in objects and len(objects['Contents']) > 0:
      pytest.fail(f"Expected all data to be deleted, but found metadata files {objects['Contents']}")

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=f'polaris_test/snowflake_catalog/db1/schema/{table_name}/data/')
    if 'Contents' in objects and len(objects['Contents']) > 0:
      pytest.fail(f"Expected all data to be deleted, but found data files {objects['Contents']}")


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials_can_write_with_random_prefix(root_client, snowflake_catalog, polaris_catalog_url, snowman,
                                                  snowman_catalog_client, test_bucket):
  """
  Update the catalog configuration to support unstructured table locations. Using snowman, create namespaces and a
  table configured to use object-store layout in a folder under the catalog root, outside of the default table
  directory. Insert into the table in multiple operations and update existing records to generate multiple metadata.json
  files and manifests. Validate the data files are present under the expected subdirectory. Delete the files afterward.

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  snowflake_catalog.properties.additional_properties['allow.unstructured.table.location'] = 'true'
  root_client.update_catalog(catalog_name=snowflake_catalog.name,
                             update_catalog_request=UpdateCatalogRequest(properties=snowflake_catalog.properties.to_dict(),
                                                                         current_entity_version=snowflake_catalog.entity_version))
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_name = f'iceberg_test_table_{str(uuid.uuid4())[-10:]}'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    spark.sql(f"CREATE TABLE {table_name} (col1 int, col2 string) TBLPROPERTIES ('write.object-storage.enabled'='true','write.data.path'='s3://{test_bucket}/polaris_test/snowflake_catalog/{table_name}data')")
    spark.sql('SHOW TABLES')

    # several inserts and an update, which should cause earlier files to show up as deleted in the later manifests
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (10, 'mystring'),
        (20, 'anotherstring'),
        (30, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (40, 'mystring'),
        (50, 'anotherstring'),
        (60, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (70, 'mystring'),
        (80, 'anotherstring'),
        (90, null)
        """)
    spark.sql(f"UPDATE {table_name} SET col2='changed string' WHERE col1 BETWEEN 20 AND 50")
    count = spark.sql(f"SELECT * FROM {table_name}").count()

    assert count == 9

    # fetch aws credentials to examine the metadata files
    response = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'), table_name,
                                                 "true")
    assert response.config is not None
    assert 's3.access-key-id' in response.config
    assert 's3.secret-access-key' in response.config
    assert 's3.session-token' in response.config

    s3 = boto3.client('s3',
                      aws_access_key_id=response.config['s3.access-key-id'],
                      aws_secret_access_key=response.config['s3.secret-access-key'],
                      aws_session_token=response.config['s3.session-token'])

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=f'polaris_test/snowflake_catalog/{table_name}data/')
    assert objects is not None
    assert len(objects['CommonPrefixes']) >= 3

    print(f"Found common prefixes in S3 {objects['CommonPrefixes']}")
    objs_to_delete = []
    for prefix in objects['CommonPrefixes']:
      data_objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                                Prefix=f'{prefix["Prefix"]}schema/{table_name}/')
      assert data_objects is not None
      print(data_objects)
      assert 'Contents' in data_objects
      objs_to_delete.extend([{'Key': obj['Key']} for obj in data_objects['Contents']])

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=f'polaris_test/snowflake_catalog/db1/schema/{table_name}/metadata/')
    assert objects is not None
    assert 'Contents' in objects
    assert len(objects['Contents']) == 15  # 5 metadata.json files, 4 manifest lists, and 6 manifests
    print(f"Found {len(objects['Contents'])} metadata files in S3 before drop")

    # use the api client to ensure the purge flag is set to true
    snowman_catalog_client.drop_table(snowflake_catalog.name,
                                      codecs.decode("1F", "hex").decode("UTF-8").join(['db1', 'schema']), table_name,
                                      purge_requested=True)
    spark.sql('DROP NAMESPACE db1.schema')
    spark.sql('DROP NAMESPACE db1')
    s3.delete_objects(Bucket=test_bucket,
                      Delete={'Objects': objs_to_delete})


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_object_store_layout_under_table_dir(root_client, snowflake_catalog, polaris_catalog_url, snowman,
                                                  snowman_catalog_client, test_bucket):
  """
  Using snowman, create namespaces and a table configured to use object-store layout, using a folder under the default
  table directory structure. Insert into the table in multiple operations and update existing records
  to generate multiple metadata.json files and manifests. Validate the data files are present under the expected
  subdirectory. Delete the files afterward.

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :param reader:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_name = f'iceberg_test_table_{str(uuid.uuid4())[-10:]}'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    table_base_dir = f'polaris_test/snowflake_catalog/db1/schema/{table_name}/obj_layout/'
    spark.sql(f"CREATE TABLE {table_name} (col1 int, col2 string) TBLPROPERTIES ('write.object-storage.enabled'='true','write.data.path'='s3://{test_bucket}/{table_base_dir}')")
    spark.sql('SHOW TABLES')

    # several inserts and an update, which should cause earlier files to show up as deleted in the later manifests
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (10, 'mystring'),
        (20, 'anotherstring'),
        (30, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (40, 'mystring'),
        (50, 'anotherstring'),
        (60, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (70, 'mystring'),
        (80, 'anotherstring'),
        (90, null)
        """)
    spark.sql(f"UPDATE {table_name} SET col2='changed string' WHERE col1 BETWEEN 20 AND 50")
    count = spark.sql(f"SELECT * FROM {table_name}").count()

    assert count == 9

    # fetch aws credentials to examine the metadata files
    response = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'), table_name,
                                                 "true")
    assert response.config is not None
    assert 's3.access-key-id' in response.config
    assert 's3.secret-access-key' in response.config
    assert 's3.session-token' in response.config

    s3 = boto3.client('s3',
                      aws_access_key_id=response.config['s3.access-key-id'],
                      aws_secret_access_key=response.config['s3.secret-access-key'],
                      aws_session_token=response.config['s3.session-token'])

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=table_base_dir)
    assert objects is not None
    assert len(objects['CommonPrefixes']) >= 3

    print(f"Found common prefixes in S3 {objects['CommonPrefixes']}")
    objs_to_delete = []
    for prefix in objects['CommonPrefixes']:
      data_objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                                Prefix=f'{prefix["Prefix"]}')
      assert data_objects is not None
      print(data_objects)
      assert 'Contents' in data_objects
      objs_to_delete.extend([{'Key': obj['Key']} for obj in data_objects['Contents']])

    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix=f'polaris_test/snowflake_catalog/db1/schema/{table_name}/metadata/')
    assert objects is not None
    assert 'Contents' in objects
    assert len(objects['Contents']) == 15  # 5 metadata.json files, 4 manifest lists, and 6 manifests
    print(f"Found {len(objects['Contents'])} metadata files in S3 before drop")

    # use the api client to ensure the purge flag is set to true
    snowman_catalog_client.drop_table(snowflake_catalog.name,
                                      codecs.decode("1F", "hex").decode("UTF-8").join(['db1', 'schema']), table_name,
                                      purge_requested=True)
    spark.sql('DROP NAMESPACE db1.schema')
    spark.sql('DROP NAMESPACE db1')
    s3.delete_objects(Bucket=test_bucket,
                      Delete={'Objects': objs_to_delete})


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
# @pytest.mark.skip(reason="This test is flaky")
def test_spark_credentials_can_create_views(snowflake_catalog, polaris_catalog_url, snowman):
  """
  Using snowman, create namespaces and a table. Insert into the table in multiple operations and update existing records
  to generate multiple metadata.json files and manifests. Create a view on the table. Verify the state of the view
  matches the state of the table.

  Using the reader principal's credentials verify read access. Validate the reader cannot insert into the table.
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    table_name = f'iceberg_test_table_{str(uuid.uuid4())[-10:]}'
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('SHOW NAMESPACES')
    spark.sql('USE db1.schema')
    spark.sql(f'CREATE TABLE {table_name} (col1 int, col2 string)')
    spark.sql('SHOW TABLES')

    # several inserts
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (10, 'mystring'),
        (20, 'anotherstring'),
        (30, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (40, 'mystring'),
        (50, 'anotherstring'),
        (60, null)
        """)
    spark.sql(f"""INSERT INTO {table_name} VALUES 
        (70, 'mystring'),
        (80, 'anotherstring'),
        (90, null)
        """)
    # verify the view reflects the current state of the table
    spark.sql(f"CREATE VIEW {table_name}_view AS SELECT col2 FROM {table_name} where col1 > 30 ORDER BY col1 DESC")
    view_records = spark.sql(f"SELECT * FROM {table_name}_view").collect()
    assert len(view_records) == 6
    assert len(view_records[0]) == 1
    assert view_records[1][0] == 'anotherstring'
    assert view_records[5][0] == 'mystring'

    # Update some records. Assert the view reflects the new state
    spark.sql(f"UPDATE {table_name} SET col2='changed string' WHERE col1 BETWEEN 20 AND 50")
    view_records = spark.sql(f"SELECT * FROM {table_name}_view").collect()
    assert len(view_records) == 6
    assert view_records[5][0] == 'changed string'


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials_s3_direct_with_write(root_client, snowflake_catalog, polaris_catalog_url,
                                                snowman, snowman_catalog_client, test_bucket):
  """
  Create two tables using Spark. Then call the loadTable api directly with snowman token to fetch the vended credentials
  for the first table.
  Verify that the credentials returned to snowman can read and write to the table's directory in S3, but don't allow
  reads or writes to the other table's directory
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param snowman_catalog_client:
  :param reader_catalog_client:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('USE db1.schema')
    spark.sql('CREATE TABLE iceberg_table (col1 int, col2 string)')
    spark.sql('CREATE TABLE iceberg_table_2 (col1 int, col2 string)')

  table2_metadata = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'),
                                                      "iceberg_table_2",
                                                      "vended-credentials").metadata_location
  response = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'), "iceberg_table",
                                               "vended-credentials")
  assert response.config is not None
  assert 's3.access-key-id' in response.config
  assert 's3.secret-access-key' in response.config
  assert 's3.session-token' in response.config

  s3 = boto3.client('s3',
                    aws_access_key_id=response.config['s3.access-key-id'],
                    aws_secret_access_key=response.config['s3.secret-access-key'],
                    aws_session_token=response.config['s3.session-token'])

  objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                            Prefix='polaris_test/snowflake_catalog/db1/schema/iceberg_table/')
  assert objects is not None
  assert 'CommonPrefixes' in objects
  assert len(objects['CommonPrefixes']) > 0

  objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                            Prefix='polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/')
  assert objects is not None
  assert 'Contents' in objects
  assert len(objects['Contents']) > 0

  metadata_file = next(f for f in objects['Contents'] if f['Key'].endswith('metadata.json'))
  assert metadata_file is not None

  metadata_contents = s3.get_object(Bucket=test_bucket, Key=metadata_file['Key'])
  assert metadata_contents is not None
  assert metadata_contents['ContentLength'] > 0

  put_object = s3.put_object(Bucket=test_bucket, Key=f"{metadata_file['Key']}.bak",
                             Body=metadata_contents['Body'].read())
  assert put_object is not None
  assert 'VersionId' in put_object
  assert put_object['VersionId'] is not None

  # list files in the other table's directory. The access policy should restrict this
  try:
    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix='polaris_test/snowflake_catalog/db1/schema/iceberg_table_2/metadata/')
    pytest.fail('Expected exception listing file outside of table directory')
  except botocore.exceptions.ClientError as error:
    print(error)

  try:
    metadata_contents = s3.get_object(Bucket=test_bucket, Key=table2_metadata)
    pytest.fail("Expected exception reading file outside of table directory")
  except botocore.exceptions.ClientError as error:
    print(error)

  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('USE db1.schema')
    spark.sql('DROP TABLE iceberg_table')
    spark.sql('DROP TABLE iceberg_table_2')
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('DROP NAMESPACE db1.schema')
    spark.sql('DROP NAMESPACE db1')


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'false').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials_s3_direct_without_write(root_client, snowflake_catalog, polaris_catalog_url,
                                                   snowman, reader_catalog_client, test_bucket):
  """
  Create two tables using Spark. Then call the loadTable api directly with test_reader token to fetch the vended
  credentials for the first table.
  Verify that the credentials returned to test_reader allow reads, but don't allow writes to the table's directory
  and don't allow reads or writes anywhere else on S3. This verifies that Polaris's authz model does not only prevent
  users from updating metadata to enforce read-only access, but uses credential scoping to enforce restrictions at
  the storage layer.
  :param root_client:
  :param snowflake_catalog:
  :param polaris_catalog_url:
  :param reader_catalog_client:
  :return:
  """
  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('CREATE NAMESPACE db1')
    spark.sql('CREATE NAMESPACE db1.schema')
    spark.sql('USE db1.schema')
    spark.sql('CREATE TABLE iceberg_table (col1 int, col2 string)')
    spark.sql('CREATE TABLE iceberg_table_2 (col1 int, col2 string)')

  table2_metadata = reader_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'),
                                                     "iceberg_table_2",
                                                     "vended-credentials").metadata_location

  response = reader_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'), "iceberg_table",
                                              "vended-credentials")
  assert response.config is not None
  assert 's3.access-key-id' in response.config
  assert 's3.secret-access-key' in response.config
  assert 's3.session-token' in response.config

  s3 = boto3.client('s3',
                    aws_access_key_id=response.config['s3.access-key-id'],
                    aws_secret_access_key=response.config['s3.secret-access-key'],
                    aws_session_token=response.config['s3.session-token'])

  objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                            Prefix='polaris_test/snowflake_catalog/db1/schema/iceberg_table/metadata/')
  assert objects is not None
  assert 'Contents' in objects
  assert len(objects['Contents']) > 0

  metadata_file = next(f for f in objects['Contents'] if f['Key'].endswith('metadata.json'))
  assert metadata_file is not None

  metadata_contents = s3.get_object(Bucket=test_bucket, Key=metadata_file['Key'])
  assert metadata_contents is not None
  assert metadata_contents['ContentLength'] > 0

  # try to write. Expect it to fail
  try:
    put_object = s3.put_object(Bucket=test_bucket, Key=f"{metadata_file['Key']}.bak",
                               Body=metadata_contents['Body'].read())
    pytest.fail("Expect exception trying to write to table directory")
  except botocore.exceptions.ClientError as error:
    print(error)

  # list files in the other table's directory. The access policy should restrict this
  try:
    objects = s3.list_objects(Bucket=test_bucket, Delimiter='/',
                              Prefix='polaris_test/snowflake_catalog/db1/schema/iceberg_table_2/metadata/')
    pytest.fail('Expected exception listing file outside of table directory')
  except botocore.exceptions.ClientError as error:
    print(error)

  try:
    metadata_contents = s3.get_object(Bucket=test_bucket, Key=table2_metadata)
    pytest.fail("Expected exception reading file outside of table directory")
  except botocore.exceptions.ClientError as error:
    print(error)

  with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                           catalog_name=snowflake_catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('USE db1.schema')
    spark.sql('DROP TABLE iceberg_table')
    spark.sql('DROP TABLE iceberg_table_2')
    spark.sql(f'USE {snowflake_catalog.name}')
    spark.sql('DROP NAMESPACE db1.schema')
    spark.sql('DROP NAMESPACE db1')


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'false').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials_s3_direct_without_read(
        snowflake_catalog, snowman_catalog_client, creator_catalog_client, test_bucket):
  """
  Create a table using `creator`, which does not have TABLE_READ_DATA and expect a `ForbiddenException`
  """
  snowman_catalog_client.create_namespace(
      prefix=snowflake_catalog.name,
      create_namespace_request=CreateNamespaceRequest(
          namespace=["some_schema"]
      )
  )

  try:
    creator_catalog_client.create_table(
        prefix=snowflake_catalog.name,
        namespace="some_schema",
        x_iceberg_access_delegation="true",
        create_table_request=CreateTableRequest(
            name="some_table",
            var_schema=ModelSchema(
                  type = 'struct',
                  fields = [],
            )
        )
    )
    pytest.fail("Expected exception when creating a table without TABLE_WRITE")
  except Exception as e:
    assert 'CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION' in str(e)

  snowman_catalog_client.drop_namespace(
    prefix=snowflake_catalog.name,
    namespace="some_schema"
  )


def create_principal(polaris_url, polaris_catalog_url, api, principal_name):
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
    if e.status == 409:
      return rotate_api.rotate_credentials(principal_name=principal_name)
    else:
      raise e

@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_credentials_s3_scoped_to_metadata_data_locations(root_client, snowflake_catalog, polaris_catalog_url,
                                                snowman, snowman_catalog_client, test_bucket):
    """
    Create a table using Spark. Then call the loadTable api directly with snowman token to fetch the vended credentials
    for the table.
    Verify that the credentials returned to snowman can only work for the location that ending with metadata or data directory
    :param root_client:
    :param snowflake_catalog:
    :param polaris_catalog_url:
    :param snowman_catalog_client:
    :param reader_catalog_client:
    :return:
    """
    with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                             catalog_name=snowflake_catalog.name,
                             polaris_url=polaris_catalog_url) as spark:
        spark.sql(f'USE {snowflake_catalog.name}')
        spark.sql('CREATE NAMESPACE db1')
        spark.sql('CREATE NAMESPACE db1.schema')
        spark.sql('USE db1.schema')
        spark.sql('CREATE TABLE iceberg_table_scope_loc(col1 int, col2 string)')
        spark.sql(f'''CREATE TABLE iceberg_table_scope_loc_slashes (col1 int, col2 string) LOCATION \'s3://{test_bucket}/polaris_test/snowflake_catalog/db1/schema/iceberg_table_scope_loc_slashes/path_with_slashes///////\'''')

    prefix1 = 'polaris_test/snowflake_catalog/db1/schema/iceberg_table_scope_loc'
    prefix2 = 'polaris_test/snowflake_catalog/db1/schema/iceberg_table_scope_loc_slashes/path_with_slashes'
    response1 = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'),
                                                        "iceberg_table_scope_loc",
                                                        "vended-credentials")
    response2 = snowman_catalog_client.load_table(snowflake_catalog.name, unquote('db1%1Fschema'),
                                                        "iceberg_table_scope_loc_slashes",
                                                        "vended-credentials")
    assert response1 is not None
    assert response2 is not None
    assert response1.metadata_location.startswith(f"s3://{test_bucket}/{prefix1}/metadata/")
    # ensure that the slashes are removed before "/metadata/"
    assert response2.metadata_location.startswith(f"s3://{test_bucket}/{prefix2}/metadata/")

    s3_1 = boto3.client('s3',
                      aws_access_key_id=response1.config['s3.access-key-id'],
                      aws_secret_access_key=response1.config['s3.secret-access-key'],
                      aws_session_token=response1.config['s3.session-token'])

    s3_2 = boto3.client('s3',
                        aws_access_key_id=response2.config['s3.access-key-id'],
                        aws_secret_access_key=response2.config['s3.secret-access-key'],
                        aws_session_token=response2.config['s3.session-token'])
    for client,prefix in [(s3_1,prefix1), (s3_2, prefix2)]:
        objects = client.list_objects(Bucket=test_bucket, Delimiter='/',
                                      Prefix=f'{prefix}/metadata/')
        assert objects is not None
        assert 'Contents' in objects , f'list metadata files failed in prefix: {prefix}/metadata/'

        objects = client.list_objects(Bucket=test_bucket, Delimiter='/',
                                      Prefix=f'{prefix}/data/')
        assert objects is not None
        # no insert executed, so should not have any data files
        assert 'Contents' not in objects , f'No contents should be in prefix: {prefix}/data/'

        objects = client.list_objects(Bucket=test_bucket, Delimiter='/',
                                  Prefix=f'{prefix}/')
        assert objects is not None
        assert 'CommonPrefixes' in objects , f'list prefixes failed in prefix: {prefix}/'
        assert len(objects['CommonPrefixes']) > 0

    with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                             catalog_name=snowflake_catalog.name,
                             polaris_url=polaris_catalog_url) as spark:
        spark.sql(f'USE {snowflake_catalog.name}')
        spark.sql('USE db1.schema')
        spark.sql('DROP TABLE iceberg_table_scope_loc PURGE')
        spark.sql('DROP TABLE iceberg_table_scope_loc_slashes PURGE')
        spark.sql(f'USE {snowflake_catalog.name}')
        spark.sql('DROP NAMESPACE db1.schema')
        spark.sql('DROP NAMESPACE db1')

@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true', reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_ctas(snowflake_catalog, polaris_catalog_url, snowman):
    """
    Create a table using CTAS and ensure that credentials are vended
    :param root_client:
    :param snowflake_catalog:
    :return:
    """
    with IcebergSparkSession(credentials=f'{snowman.principal.client_id}:{snowman.credentials.client_secret}',
                             catalog_name=snowflake_catalog.name,
                             polaris_url=polaris_catalog_url) as spark:
        table_name = f'iceberg_test_table_{str(uuid.uuid4())[-10:]}'
        spark.sql(f'USE {snowflake_catalog.name}')
        spark.sql('CREATE NAMESPACE db1')
        spark.sql('CREATE NAMESPACE db1.schema')
        spark.sql('USE db1.schema')
        spark.sql(f'CREATE TABLE {table_name}_t1 (col1 int)')
        spark.sql('SHOW TABLES')

        # Insert some data
        spark.sql(f"INSERT INTO {table_name}_t1 VALUES (10)")

        # Run CTAS
        spark.sql(f"CREATE TABLE {table_name}_t2 AS SELECT * FROM {table_name}_t1")


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


def create_principal_role(api, role_name):
  principal_role = PrincipalRole(name=role_name)
  try:
    api.create_principal_role(CreatePrincipalRoleRequest(principal_role=principal_role))
    return api.get_principal_role(principal_role_name=role_name)
  except ApiException as e:
    return api.get_principal_role(principal_role_name=role_name)
