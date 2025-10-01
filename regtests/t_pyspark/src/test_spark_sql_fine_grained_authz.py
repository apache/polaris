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

"""
Fine-grained authorization tests for Polaris.

These tests validate that fine-grained table metadata update privileges work correctly.

The authorization logic is storage-agnostic, so testing with a catalog using FILE storage
"""

import os
import pytest
import uuid
from py4j.protocol import Py4JJavaError

from iceberg_spark import IcebergSparkSession
from polaris.management import PrincipalRole, CatalogRole, CatalogGrant, CatalogPrivilege, \
    AddGrantRequest, GrantCatalogRoleRequest, GrantPrincipalRoleRequest

# Import existing helper functions instead of copying them
from conftest import create_catalog_role
from test_spark_sql_s3_with_privileges import create_principal, create_principal_role


def _create_fine_grained_principal(polaris_url, polaris_catalog_url, root_client, catalog):
    """
    Helper function to create a principal for fine-grained authorization testing.
    Creates two catalog roles:
    1. Setup role: Has privileges needed to create tables/namespaces (including TABLE_WRITE_DATA)
    2. Update role: Has only basic privileges (no super-privileges) - for testing fine-grained updates
    
    The principal gets both roles. Tests should grant specific fine-grained privileges 
    to the update role only.
    """
    catalog_name = catalog.name
    principal_name = f"fine_grained_user_{str(uuid.uuid4())[-10:]}"
    principal_role_name = f"fine_grained_role_{str(uuid.uuid4())[-10:]}"
    setup_catalog_role_name = f"setup_cat_role_{str(uuid.uuid4())[-10:]}"
    update_catalog_role_name = f"update_cat_role_{str(uuid.uuid4())[-10:]}"
    
    try:
        # Create principal and roles using existing helper functions
        principal = create_principal(polaris_url, polaris_catalog_url, root_client, principal_name)
        writer_principal_role = create_principal_role(root_client, principal_role_name)
        setup_catalog_role = create_catalog_role(root_client, catalog, setup_catalog_role_name)
        update_catalog_role = create_catalog_role(root_client, catalog, update_catalog_role_name)
        
        # Assign both catalog roles to the principal role
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=writer_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=setup_catalog_role)
        )
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=writer_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=update_catalog_role)
        )
        
        # Grant setup privileges to the setup role (includes super-privileges needed for table creation)
        setup_privileges = [
            CatalogPrivilege.NAMESPACE_FULL_METADATA,
            CatalogPrivilege.TABLE_CREATE, 
            CatalogPrivilege.TABLE_LIST,
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_DROP,
            CatalogPrivilege.TABLE_WRITE_DATA  # Needed for CREATE TABLE with write delegation
        ]
        
        for privilege in setup_privileges:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                setup_catalog_role_name,
                AddGrantRequest(grant=CatalogGrant(
                    catalog_name=catalog_name,
                    type='catalog',
                    privilege=privilege
                ))
            )
        
        # Grant basic privileges to the update role (no super-privileges)
        update_basic_privileges = [
            CatalogPrivilege.TABLE_READ_PROPERTIES,
        ]
        
        for privilege in update_basic_privileges:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                update_catalog_role_name,
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
        
        return {
            'principal': principal,
            'catalog_name': catalog_name,
            'setup_catalog_role_name': setup_catalog_role_name,
            'update_catalog_role_name': update_catalog_role_name,
            'principal_role_name': principal_role_name
        }, principal_name, principal_role_name, setup_catalog_role_name, update_catalog_role_name
        
    except Exception as e:
        # Cleanup on error
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, setup_catalog_role_name)
            root_client.delete_catalog_role(catalog_name, update_catalog_role_name)
        except:
            pass
        raise e


def test_coarse_grained_table_write_properties(polaris_url, polaris_catalog_url, root_client, fine_grained_catalog):
    """Test that coarse-grained TABLE_WRITE_PROPERTIES privilege allows all metadata operations"""
    
    # Create principal for this test
    principal_info, principal_name, principal_role_name, setup_catalog_role_name, update_catalog_role_name = _create_fine_grained_principal(
        polaris_url, polaris_catalog_url, root_client, fine_grained_catalog
    )
    
    try:
        principal = principal_info['principal']
        catalog_name = principal_info['catalog_name']
        
        # Grant broad TABLE_WRITE_PROPERTIES privilege to the test role
        root_client.add_grant_to_catalog_role(
            catalog_name,
            update_catalog_role_name,
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
    finally:
        # Cleanup principal and roles
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, setup_catalog_role_name)
            root_client.delete_catalog_role(catalog_name, update_catalog_role_name)
        except:
            pass


def test_fine_grained_table_set_properties(polaris_url, polaris_catalog_url, root_client, fine_grained_catalog):
    """Test fine-grained TABLE_SET_PROPERTIES privilege allows SET operations but not UNSET"""
    
    catalog_name = fine_grained_catalog.name
    
    # Create setup principal (for table creation)
    setup_principal_name = f"setup_user_{str(uuid.uuid4())[-10:]}"
    setup_principal_role_name = f"setup_role_{str(uuid.uuid4())[-10:]}"
    setup_catalog_role_name = f"setup_cat_role_{str(uuid.uuid4())[-10:]}"
    
    # Create test principal (for fine-grained testing)
    test_principal_name = f"test_user_{str(uuid.uuid4())[-10:]}"
    test_principal_role_name = f"test_role_{str(uuid.uuid4())[-10:]}"
    test_catalog_role_name = f"test_cat_role_{str(uuid.uuid4())[-10:]}"
    
    try:
        # Create setup principal with full privileges
        setup_principal = create_principal(polaris_url, polaris_catalog_url, root_client, setup_principal_name)
        setup_principal_role = create_principal_role(root_client, setup_principal_role_name)
        setup_catalog_role = create_catalog_role(root_client, fine_grained_catalog, setup_catalog_role_name)
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=setup_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=setup_catalog_role)
        )
        
        # Grant setup privileges (including super-privileges)
        setup_privileges = [
            CatalogPrivilege.NAMESPACE_FULL_METADATA,
            CatalogPrivilege.TABLE_CREATE, 
            CatalogPrivilege.TABLE_LIST,
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_DROP,
            CatalogPrivilege.TABLE_WRITE_DATA
        ]
        
        for privilege in setup_privileges:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                setup_catalog_role_name,
                AddGrantRequest(grant=CatalogGrant(
                    catalog_name=catalog_name,
                    type='catalog',
                    privilege=privilege
                ))
            )
        
        root_client.assign_principal_role(
            setup_principal.principal.name,
            grant_principal_role_request=GrantPrincipalRoleRequest(principal_role=setup_principal_role)
        )
        
        # Create test principal with only fine-grained privileges
        test_principal = create_principal(polaris_url, polaris_catalog_url, root_client, test_principal_name)
        test_principal_role = create_principal_role(root_client, test_principal_role_name)
        test_catalog_role = create_catalog_role(root_client, fine_grained_catalog, test_catalog_role_name)
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=test_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=test_catalog_role)
        )
        
        # Grant only basic privileges to test role
        test_basic_privileges = [
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_READ_DATA,  # Needed to load table for operations
            CatalogPrivilege.TABLE_SET_PROPERTIES  # The specific privilege we're testing
        ]
        
        for privilege in test_basic_privileges:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                test_catalog_role_name,
                AddGrantRequest(grant=CatalogGrant(
                    catalog_name=catalog_name,
                    type='catalog',
                    privilege=privilege
                ))
            )
        
        root_client.assign_principal_role(
            test_principal.principal.name,
            grant_principal_role_request=GrantPrincipalRoleRequest(principal_role=test_principal_role)
        )
        
        # Create table using the setup principal
        with IcebergSparkSession(
            credentials=f'{setup_principal.principal.client_id}:{setup_principal.credentials.client_secret.get_secret_value()}',
            catalog_name=catalog_name,
            polaris_url=polaris_catalog_url
        ) as spark:
            spark.sql(f'USE {catalog_name}')
            spark.sql('CREATE NAMESPACE db1')
            spark.sql('CREATE TABLE db1.test_table (col1 int, col2 string)')
        
        # Test fine-grained operations using the test principal
        with IcebergSparkSession(
            credentials=f'{test_principal.principal.client_id}:{test_principal.credentials.client_secret.get_secret_value()}',
            catalog_name=catalog_name,
            polaris_url=polaris_catalog_url
        ) as spark:
            spark.sql(f'USE {catalog_name}')
            
            # SET operation should work with TABLE_SET_PROPERTIES
            spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('test.property' = 'test.value')")
            print("✅ SET TBLPROPERTIES succeeded with TABLE_SET_PROPERTIES")
            
            # UNSET operation should fail without TABLE_REMOVE_PROPERTIES
            try:
                spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('test.property')")
                print("❌ UNSET succeeded without TABLE_REMOVE_PROPERTIES - TEST FAILED")
                assert False, "UNSET should have failed without TABLE_REMOVE_PROPERTIES"
            except Py4JJavaError as e:
                print("✅ UNSET correctly failed without TABLE_REMOVE_PROPERTIES")
                error_str = str(e).lower()
                assert "forbidden" in error_str or "not authorized" in error_str, f"Unexpected error: {e}"
    finally:
        # Cleanup principals and roles
        try:
            root_client.delete_principal(setup_principal_name)
            root_client.delete_principal_role(setup_principal_role_name)
            root_client.delete_catalog_role(catalog_name, setup_catalog_role_name)
            root_client.delete_principal(test_principal_name)
            root_client.delete_principal_role(test_principal_role_name)
            root_client.delete_catalog_role(catalog_name, test_catalog_role_name)
        except:
            pass


def test_fine_grained_table_remove_properties(polaris_url, polaris_catalog_url, root_client, fine_grained_catalog):
    """Test that fine-grained TABLE_REMOVE_PROPERTIES privilege allows UNSET operations"""
    
    # Create principal for this test
    principal_info, principal_name, principal_role_name, setup_catalog_role_name, update_catalog_role_name = _create_fine_grained_principal(
        polaris_url, polaris_catalog_url, root_client, fine_grained_catalog
    )
    
    try:
        principal = principal_info['principal']
        catalog_name = principal_info['catalog_name']
        
        # Grant both TABLE_SET_PROPERTIES and TABLE_REMOVE_PROPERTIES
        for privilege in [CatalogPrivilege.TABLE_SET_PROPERTIES, CatalogPrivilege.TABLE_REMOVE_PROPERTIES]:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                update_catalog_role_name,
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
    finally:
        # Cleanup principal and roles
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, setup_catalog_role_name)
            root_client.delete_catalog_role(catalog_name, update_catalog_role_name)
        except:
            pass


def test_multiple_fine_grained_privileges_together(polaris_url, polaris_catalog_url, root_client, fine_grained_catalog):
    """Test that multiple fine-grained privileges work together correctly"""
    
    # Create principal for this test
    principal_info, principal_name, principal_role_name, setup_catalog_role_name, update_catalog_role_name = _create_fine_grained_principal(
        polaris_url, polaris_catalog_url, root_client, fine_grained_catalog
    )
    
    try:
        principal = principal_info['principal']
        catalog_name = principal_info['catalog_name']
        
        # Grant both specific fine-grained privileges
        for privilege in [CatalogPrivilege.TABLE_SET_PROPERTIES, CatalogPrivilege.TABLE_REMOVE_PROPERTIES]:
            root_client.add_grant_to_catalog_role(
                catalog_name,
                update_catalog_role_name,
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
    finally:
        # Cleanup principal and roles
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, setup_catalog_role_name)
            root_client.delete_catalog_role(catalog_name, update_catalog_role_name)
        except:
            pass


def test_fine_grained_insufficient_permissions(polaris_url, polaris_catalog_url, root_client, fine_grained_catalog):
    """Test that having only one of the required privileges fails when both are needed"""
    
    # Create principal for this test
    principal_info, principal_name, principal_role_name, setup_catalog_role_name, update_catalog_role_name = _create_fine_grained_principal(
        polaris_url, polaris_catalog_url, root_client, fine_grained_catalog
    )
    
    try:
        principal = principal_info['principal']
        catalog_name = principal_info['catalog_name']
        
        # Grant only TABLE_SET_PROPERTIES (not REMOVE_PROPERTIES)
        root_client.add_grant_to_catalog_role(
            catalog_name,
            update_catalog_role_name,
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
            
            # SET operation should work
            spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('prop1' = 'value1')")
            
            # UNSET operation should fail without TABLE_REMOVE_PROPERTIES
            with pytest.raises(Py4JJavaError) as exc_info:
                spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('prop1')")
            
            # Verify the error is related to authorization
            assert "not authorized" in str(exc_info.value).lower() or "forbidden" in str(exc_info.value).lower()
            
            # Cleanup
            spark.sql('DROP TABLE db1.test_table PURGE')
            spark.sql('DROP NAMESPACE db1')
    finally:
        # Cleanup principal and roles
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, setup_catalog_role_name)
            root_client.delete_catalog_role(catalog_name, update_catalog_role_name)
        except:
            pass
