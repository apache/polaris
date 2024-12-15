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

import os
import pytest
from iceberg_spark import IcebergSparkSession
from polaris.management import *
from conftest import create_principal, create_principal_role, create_catalog_role, clear_namespace
from pyspark.sql import Row


@pytest.fixture
def catalog(root_client, catalog_client, test_bucket, aws_role_arn):
  """
  Fixture that creates and cleans up a test catalog in Polaris.

  Args:
    root_client: The root client for Polaris.
    catalog_client: The catalog client for Polaris.
    test_bucket: The AWS S3 bucket.
    aws_role_arn: The AWS role ARN.

  Yields:
    Catalog: The created test catalog.
  """
  storage_conf = AwsStorageConfigInfo(
    storage_type="S3",
    allowed_locations=[f"s3://{test_bucket}/polaris_test/"],
    role_arn=aws_role_arn
  )
  catalog_name = 'spark_sql_s3_catalog'
  catalog = Catalog(
    name=catalog_name,
    type='INTERNAL',
    properties={
      "default-base-location": f"s3://{test_bucket}/polaris_test/{catalog_name}",
      "client.credentials-provider": "software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider"
    },
    storage_config_info=storage_conf
  )
  catalog.storage_config_info = storage_conf
  try:
    root_client.create_catalog(create_catalog_request=CreateCatalogRequest(catalog=catalog))
    yield root_client.get_catalog(catalog_name=catalog.name)
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
def test_user(polaris_url, polaris_catalog_url, root_client, catalog):
  """
  Fixture that creates a test user, assigns necessary roles and grants, and cleans up afterward.

  Args:
    polaris_url: The URL for Polaris.
    polaris_catalog_url: The catalog URL for Polaris.
    root_client: The root client for Polaris.
    catalog: The test catalog to associate the user with.

  Yields:
    Principal: The created test user principal.
  """
  principal = "test_user"
  principal_role = "test_user_principal_role"
  catalog_role = "test_user_catalog_role"

  def assign_grants_to_catalog_role():
    """
    Assigns grants to the test user catalog role for necessary permissions.
    """
    grants = [
      CatalogGrant(catalog_name=catalog.name, type='catalog', privilege=CatalogPrivilege.TABLE_FULL_METADATA),
      CatalogGrant(catalog_name=catalog.name, type='catalog', privilege=CatalogPrivilege.VIEW_FULL_METADATA),
      CatalogGrant(catalog_name=catalog.name, type='catalog', privilege=CatalogPrivilege.TABLE_WRITE_DATA),
      CatalogGrant(catalog_name=catalog.name, type='catalog', privilege=CatalogPrivilege.NAMESPACE_FULL_METADATA)
    ]
    for grant in grants:
      root_client.add_grant_to_catalog_role(catalog.name, writer_catalog_role.name,
                                            AddGrantRequest(grant=grant))

  try:
    test_user = create_principal(polaris_url, polaris_catalog_url, root_client, principal)
    writer_principal_role = create_principal_role(root_client, principal_role)
    writer_catalog_role = create_catalog_role(root_client, catalog, catalog_role)
    root_client.assign_catalog_role_to_principal_role(principal_role_name=writer_principal_role.name,
                                                      catalog_name=catalog.name,
                                                      grant_catalog_role_request=GrantCatalogRoleRequest(
                                                        catalog_role=writer_catalog_role))
    assign_grants_to_catalog_role()
    root_client.assign_principal_role(test_user.principal.name,
                                      grant_principal_role_request=GrantPrincipalRoleRequest(
                                        principal_role=writer_principal_role))
    yield test_user
  finally:
    root_client.delete_principal(principal)
    root_client.delete_principal_role(principal_role_name=principal_role)
    root_client.delete_catalog_role(catalog_role_name=catalog_role, catalog_name=catalog.name)


@pytest.mark.skipif(os.environ.get('AWS_TEST_ENABLED', 'False').lower() != 'true',
                    reason='AWS_TEST_ENABLED is not set or is false')
def test_spark_sql_basic(root_client, catalog, polaris_catalog_url, test_user):
  """
  Test the basic operations of Spark SQL with the Polaris catalog. The test checks
  namespace creation, table operations (create, insert, select, update), and view creation.

  Args:
    root_client: The root client for Polaris.
    catalog: The test catalog to be used for operations.
    polaris_catalog_url: The URL for Polaris catalog.
    test_user: The test user with appropriate privileges.
  """
  with IcebergSparkSession(credentials=f'{test_user.principal.client_id}:{test_user.credentials.client_secret}',
                           catalog_name=catalog.name,
                           polaris_url=polaris_catalog_url) as spark:
    # Test namespace creation and listing
    spark.sql(f'USE {catalog.name}')
    namespaces = spark.sql('show namespaces').collect()
    assert namespaces == []
    spark.sql('create namespace db1')
    spark.sql('create namespace db2')
    namespaces = spark.sql('show namespaces').collect()
    assert namespaces == [Row(namespace='db1'), Row(namespaces='db2')]

    # Test nested namespace creation and listing
    spark.sql('create namespace db1.schema1')
    namespaces = spark.sql('show namespaces').collect()
    assert namespaces == [Row(namespace='db1'), Row(namespaces='db2')]
    namespaces = spark.sql('show namespaces in db1').collect()
    assert namespaces == [Row(namespace='db1.schema1')]

    # Test table creation and listing
    spark.sql('create table db1.schema1.tbl1 (col1 int, col2 string)')
    tables = spark.sql('show tables in db1').collect()
    assert tables == []
    tables = spark.sql('show tables in db1.schema1').collect()
    assert tables == [Row(namespace='db1.schema1', tableName='tbl1', isTemporary=False)]
    spark.sql('use db1.schema1')
    tables = spark.sql('show tables').collect()
    assert tables == [Row(namespace='db1.schema1', tableName='tbl1', isTemporary=False)]

    # Test inserting data into a table and selecting it
    spark.sql('insert into tbl1 values (123, "hello"), (234, "world")')
    data = spark.sql('select * from tbl1').collect()
    assert data == [Row(col1=123, col2='hello'), Row(col1=234, col2='world')]

    # Test creating a view and selecting from it
    spark.sql('create view db1.schema1.v1 (strcol) as select col2 from tbl1 order by col1 DESC')
    views = spark.sql('show views in db1.schema1').collect()
    assert views == [Row(namespace='db1.schema1', viewName='v1', isTemporary=False)]
    data = spark.sql('select * from v1').collect()
    assert data == [Row(strcol='world'), Row(strcol='hello')]

    # Test updating data in a table via the view
    spark.sql('update tbl1 set col2 = "world2" where col1 = 234')
    data = spark.sql('select * from v1').collect()
    assert data == [Row(strcol='world2'), Row(strcol='hello')]

    # Test dropping the view, table, and namespaces
    spark.sql('drop view v1')
    spark.sql('drop table tbl1 purge')
    tables = spark.sql('show tables').collect()
    assert tables == []
    spark.sql('drop namespace db1.schema1')
    spark.sql('drop namespace db1')
    namespaces = spark.sql('show namespaces').collect()
    assert namespaces == [Row(namespace='db2')]
    spark.sql('drop namespace db2')
    namespaces = spark.sql('show namespaces').collect()
    assert namespaces == []
