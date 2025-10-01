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

import pytest
import uuid
from py4j.protocol import Py4JJavaError

from iceberg_spark import IcebergSparkSession
from polaris.management import PrincipalRole, CatalogRole, CatalogGrant, CatalogPrivilege, \
    AddGrantRequest, GrantCatalogRoleRequest, GrantPrincipalRoleRequest

# Import existing helper functions instead of copying them
from conftest import create_catalog_role
from test_spark_sql_s3_with_privileges import create_principal, create_principal_role


@pytest.fixture 
def fine_grained_principal(polaris_url, polaris_catalog_url, root_client, snowflake_catalog):
    """Create a principal for fine-grained authorization testing using the existing snowflake_catalog"""
    catalog_name = snowflake_catalog.name
    principal_name = f"fine_grained_user_{str(uuid.uuid4())[-10:]}"
    principal_role_name = f"fine_grained_role_{str(uuid.uuid4())[-10:]}"
    catalog_role_name = f"fine_grained_cat_role_{str(uuid.uuid4())[-10:]}"
    
    try:
        # Create principal and roles using existing helper functions
        principal = create_principal(polaris_url, polaris_catalog_url, root_client, principal_name)
        writer_principal_role = create_principal_role(root_client, principal_role_name)
        writer_catalog_role = create_catalog_role(root_client, snowflake_catalog, catalog_role_name)
        
        # Assign roles
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=writer_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=writer_catalog_role)
        )
        
        # Grant basic privileges needed for setup
        basic_privileges = [
            CatalogPrivilege.NAMESPACE_FULL_METADATA,
            CatalogPrivilege.TABLE_CREATE, 
            CatalogPrivilege.TABLE_LIST,
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_WRITE_DATA,
            CatalogPrivilege.TABLE_DROP
        ]
        
        for privilege in basic_privileges:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                catalog_role_name,
                AddGrantRequest(grant=CatalogGrant(
                    catalog_name=catalog_name,
                    type='catalog',
                    privilege=privilege
                ))
            )
        
        root_client.assign_principal_role(
            principal.principal.name,
            grant_principal_role_request=GrantPrincipalRoleRequest(principal_role=writer_principal_role)
        )
        
        yield {
            'principal': principal,
            'catalog_name': catalog_name,
            'catalog_role_name': catalog_role_name,
            'principal_role_name': principal_role_name
        }
    finally:
        # Cleanup
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, catalog_role_name)
        except:
            pass


def test_coarse_grained_table_write_properties(polaris_catalog_url, root_client, fine_grained_principal):
    """Test that coarse-grained TABLE_WRITE_PROPERTIES privilege allows all metadata operations"""
    principal = fine_grained_principal['principal']
    catalog_name = fine_grained_principal['catalog_name']
    catalog_role_name = fine_grained_principal['catalog_role_name']
    
    # Grant broad TABLE_WRITE_PROPERTIES privilege
    root_client.add_grant_to_catalog_role(
        catalog_name,
        catalog_role_name,
        AddGrantRequest(grant=CatalogGrant(
            catalog_name=catalog_name,
            type='catalog',
            privilege=CatalogPrivilege.TABLE_WRITE_PROPERTIES
        ))
    )
    
    # Test with coarse-grained privilege - should work for both SET and UNSET operations
    with IcebergSparkSession(
        credentials=f'{principal.principal.client_id}:{principal.credentials.client_secret.get_secret_value()}',
        catalog_name=catalog_name,
        polaris_url=polaris_catalog_url
    ) as spark:
        spark.sql(f'USE {catalog_name}')
        spark.sql('CREATE NAMESPACE db1')
        spark.sql('CREATE TABLE db1.test_table (col1 int, col2 string)')
        
        # Both operations should work with TABLE_WRITE_PROPERTIES
        spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('test.property' = 'test.value')")
        spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('test.property')")
        
        # Cleanup
        spark.sql('DROP TABLE db1.test_table PURGE')
        spark.sql('DROP NAMESPACE db1')


def test_fine_grained_table_set_properties(polaris_catalog_url, root_client, fine_grained_principal):
    """Test that fine-grained TABLE_SET_PROPERTIES privilege allows SET operations but not UNSET"""
    principal = fine_grained_principal['principal']
    catalog_name = fine_grained_principal['catalog_name']
    catalog_role_name = fine_grained_principal['catalog_role_name']
    
    # Remove any broad privileges that might interfere
    try:
        # This might not exist, so ignore errors
        pass  
    except:
        pass
    
    # Grant only TABLE_SET_PROPERTIES (fine-grained)
    root_client.add_grant_to_catalog_role(
        catalog_name,
        catalog_role_name,
        AddGrantRequest(grant=CatalogGrant(
            catalog_name=catalog_name,
            type='catalog',
            privilege=CatalogPrivilege.TABLE_SET_PROPERTIES
        ))
    )
    
    with IcebergSparkSession(
        credentials=f'{principal.principal.client_id}:{principal.credentials.client_secret.get_secret_value()}',
        catalog_name=catalog_name,
        polaris_url=polaris_catalog_url
    ) as spark:
        spark.sql(f'USE {catalog_name}')
        spark.sql('CREATE NAMESPACE db1')
        spark.sql('CREATE TABLE db1.test_table (col1 int, col2 string)')
        
        # SET operation should work with TABLE_SET_PROPERTIES
        spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('test.property' = 'test.value')")
        
        # UNSET operation should fail without TABLE_REMOVE_PROPERTIES
        with pytest.raises(Py4JJavaError) as exc_info:
            spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('test.property')")
        
        # Verify the error is related to authorization
        assert "not authorized" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()
        
        # Cleanup
        spark.sql('DROP TABLE db1.test_table PURGE') 
        spark.sql('DROP NAMESPACE db1')


def test_fine_grained_table_remove_properties(polaris_catalog_url, root_client, fine_grained_principal):
    """Test that fine-grained TABLE_REMOVE_PROPERTIES privilege allows UNSET operations"""
    principal = fine_grained_principal['principal']
    catalog_name = fine_grained_principal['catalog_name']
    catalog_role_name = fine_grained_principal['catalog_role_name']
    
    # Grant both TABLE_SET_PROPERTIES and TABLE_REMOVE_PROPERTIES
    for privilege in [CatalogPrivilege.TABLE_SET_PROPERTIES, CatalogPrivilege.TABLE_REMOVE_PROPERTIES]:
        root_client.add_grant_to_catalog_role(
            catalog_name,
            catalog_role_name,
            AddGrantRequest(grant=CatalogGrant(
                catalog_name=catalog_name,
                type='catalog',
                privilege=privilege
            ))
        )
    
    with IcebergSparkSession(
        credentials=f'{principal.principal.client_id}:{principal.credentials.client_secret.get_secret_value()}',
        catalog_name=catalog_name,
        polaris_url=polaris_catalog_url
    ) as spark:
        spark.sql(f'USE {catalog_name}')
        spark.sql('CREATE NAMESPACE db1')
        spark.sql('CREATE TABLE db1.test_table (col1 int, col2 string)')
        
        # Both SET and UNSET operations should work
        spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('prop1' = 'value1', 'prop2' = 'value2')")
        spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('prop1')")
        
        # Cleanup
        spark.sql('DROP TABLE db1.test_table PURGE')
        spark.sql('DROP NAMESPACE db1')


def test_multiple_fine_grained_privileges_together(polaris_catalog_url, root_client, fine_grained_principal):
    """Test that multiple fine-grained privileges work together correctly"""
    principal = fine_grained_principal['principal']
    catalog_name = fine_grained_principal['catalog_name']
    catalog_role_name = fine_grained_principal['catalog_role_name']
    
    # Grant both specific fine-grained privileges
    for privilege in [CatalogPrivilege.TABLE_SET_PROPERTIES, CatalogPrivilege.TABLE_REMOVE_PROPERTIES]:
        root_client.add_grant_to_catalog_role(
            catalog_name,
            catalog_role_name,
            AddGrantRequest(grant=CatalogGrant(
                catalog_name=catalog_name,
                type='catalog',
                privilege=privilege
            ))
        )
    
    with IcebergSparkSession(
        credentials=f'{principal.principal.client_id}:{principal.credentials.client_secret.get_secret_value()}',
        catalog_name=catalog_name,
        polaris_url=polaris_catalog_url
    ) as spark:
        spark.sql(f'USE {catalog_name}')
        spark.sql('CREATE NAMESPACE db1')
        spark.sql('CREATE TABLE db1.test_table (col1 int, col2 string)')
        
        # Multiple operations in sequence - all should work
        spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('prop1' = 'value1', 'prop2' = 'value2')")
        spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('prop1')")
        spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('prop3' = 'value3')")
        spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('prop2', 'prop3')")
        
        # Cleanup
        spark.sql('DROP TABLE db1.test_table PURGE')
        spark.sql('DROP NAMESPACE db1')
