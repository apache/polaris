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
from apache_polaris.sdk.management import PrincipalRole, CatalogRole, CatalogGrant, CatalogPrivilege, \
    AddGrantRequest, GrantCatalogRoleRequest, GrantPrincipalRoleRequest

# Import existing helper functions instead of copying them
from conftest import create_catalog_role
from test_spark_sql_s3_with_privileges import create_principal, create_principal_role


@pytest.fixture
def fine_grained_authz_test_catalog(root_client, catalog_client):
    """
    Catalog specifically for fine-grained authorization testing.
    Does NOT assign catalog_admin to service_admin to avoid privilege inheritance issues.
    """
    from apache_polaris.sdk.management import FileStorageConfigInfo, Catalog, CatalogProperties, CreateCatalogRequest
    from conftest import create_catalog_role
    
    catalog_name = f'fine_grained_authz_test_catalog_{str(uuid.uuid4())[-10:]}'
    storage_config = FileStorageConfigInfo(storage_type="FILE", allowed_locations=["file:///tmp"])
    base_location = "file:///tmp/polaris"
    
    # Build properties dict with fine-grained authorization enabled
    catalog_properties = {
        "default-base-location": base_location,
        "polaris.config.drop-with-purge.enabled": "true",
        "polaris.config.enable-fine-grained-update-table-privileges": "true"
    }
    
    catalog = Catalog(name=catalog_name, type='INTERNAL', 
                      properties=CatalogProperties.from_dict(catalog_properties),
                      storage_config_info=storage_config)
    
    try:
        root_client.create_catalog(create_catalog_request=CreateCatalogRequest(catalog=catalog))
        resp = root_client.get_catalog(catalog_name=catalog.name)
        
        # IMPORTANT: We do NOT assign catalog_admin to service_admin here!
        # This ensures fine-grained tests have only the privileges explicitly granted
        
        # However, we need to grant cleanup privileges to service_admin for fixture teardown
        cleanup_catalog_role = create_catalog_role(root_client, resp, 'cleanup_role')
        cleanup_privileges = [
            CatalogPrivilege.TABLE_DROP,
            CatalogPrivilege.TABLE_WRITE_DATA,  # Needed for DROP_TABLE_WITH_PURGE
            CatalogPrivilege.NAMESPACE_DROP
        ]
        
        for privilege in cleanup_privileges:
            root_client.add_grant_to_catalog_role(
                catalog_name, cleanup_catalog_role.name,
                AddGrantRequest(grant=CatalogGrant(
                    catalog_name=catalog_name,
                    type='catalog',
                    privilege=privilege
                ))
            )
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name='service_admin',
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=cleanup_catalog_role)
        )
        
        yield resp
    finally:
        # Cleanup
        from conftest import clear_namespace
        namespaces = catalog_client.list_namespaces(catalog_name)
        for n in namespaces.namespaces:
            clear_namespace(catalog_name, catalog_client, n)
        catalog_roles = root_client.list_catalog_roles(catalog_name)
        for r in catalog_roles.roles:
            if r.name not in ['catalog_admin', 'cleanup_role']:
                root_client.delete_catalog_role(catalog_name, r.name)
        # Delete cleanup_role last
        try:
            root_client.delete_catalog_role(catalog_name, 'cleanup_role')
        except:
            pass
        root_client.delete_catalog(catalog_name=catalog_name)



def test_coarse_grained_table_write_properties(polaris_url, polaris_catalog_url, root_client, fine_grained_authz_test_catalog):
    """Test that coarse-grained TABLE_WRITE_PROPERTIES privilege allows all metadata operations"""
    
    catalog_name = fine_grained_authz_test_catalog.name
    
    # Create a single principal with TABLE_WRITE_PROPERTIES (coarse-grained privilege)
    principal_name = f"coarse_grained_user_{str(uuid.uuid4())[-10:]}"
    principal_role_name = f"coarse_grained_role_{str(uuid.uuid4())[-10:]}"
    catalog_role_name = f"coarse_grained_cat_role_{str(uuid.uuid4())[-10:]}"
    
    try:
        # Create principal with coarse-grained privileges
        principal = create_principal(polaris_url, polaris_catalog_url, root_client, principal_name)
        principal_role = create_principal_role(root_client, principal_role_name)
        catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, catalog_role_name)
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=catalog_role)
        )
        
        # Grant coarse-grained privileges (including TABLE_WRITE_PROPERTIES super-privilege)
        coarse_grained_privileges = [
            CatalogPrivilege.NAMESPACE_FULL_METADATA,
            CatalogPrivilege.TABLE_CREATE, 
            CatalogPrivilege.TABLE_LIST,
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_READ_DATA,
            CatalogPrivilege.TABLE_DROP,
            CatalogPrivilege.TABLE_WRITE_DATA,
            CatalogPrivilege.TABLE_WRITE_PROPERTIES  # This should allow both SET and UNSET
        ]
        
        for privilege in coarse_grained_privileges:
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
            grant_principal_role_request=GrantPrincipalRoleRequest(principal_role=principal_role)
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
            
    finally:
        # Cleanup principal and roles
        try:
            root_client.delete_principal(principal_name)
            root_client.delete_principal_role(principal_role_name)
            root_client.delete_catalog_role(catalog_name, catalog_role_name)
        except:
            pass


def test_fine_grained_table_set_properties(polaris_url, polaris_catalog_url, root_client, fine_grained_authz_test_catalog):
    """Test fine-grained TABLE_SET_PROPERTIES privilege allows SET operations but not UNSET"""
    
    catalog_name = fine_grained_authz_test_catalog.name
    
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
        setup_catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, setup_catalog_role_name)
        
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
        test_catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, test_catalog_role_name)
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=test_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=test_catalog_role)
        )
        
        # Grant only basic privileges to test role
        test_basic_privileges = [
            CatalogPrivilege.TABLE_READ_PROPERTIES,
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
            polaris_url=polaris_catalog_url,
            use_vended_credentials=False  # Not needed for file storage type
        ) as spark:
            spark.sql(f'USE {catalog_name}')
            
            # SET operation should work with TABLE_SET_PROPERTIES
            spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('test.property' = 'test.value')")
            
            # UNSET operation should fail without TABLE_REMOVE_PROPERTIES
            with pytest.raises(Py4JJavaError) as exc_info:
                spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('test.property')")
            
            # Verify the error is related to authorization
            error_str = str(exc_info.value).lower()
            assert "forbidden" in error_str or "not authorized" in error_str, f"Unexpected error: {exc_info.value}"
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


def test_fine_grained_table_remove_properties(polaris_url, polaris_catalog_url, root_client, fine_grained_authz_test_catalog):
    """Test that fine-grained TABLE_REMOVE_PROPERTIES privilege allows UNSET operations but not SET"""
    
    catalog_name = fine_grained_authz_test_catalog.name
    
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
        setup_catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, setup_catalog_role_name)
        
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
        test_catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, test_catalog_role_name)
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=test_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=test_catalog_role)
        )
        
        # Grant only TABLE_REMOVE_PROPERTIES (not SET_PROPERTIES)
        test_basic_privileges = [
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_REMOVE_PROPERTIES  # The specific privilege we're testing
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
        
        # Create table using the setup principal and set a property to remove later
        with IcebergSparkSession(
            credentials=f'{setup_principal.principal.client_id}:{setup_principal.credentials.client_secret.get_secret_value()}',
            catalog_name=catalog_name,
            polaris_url=polaris_catalog_url
        ) as spark:
            spark.sql(f'USE {catalog_name}')
            spark.sql('CREATE NAMESPACE db1')
            spark.sql('CREATE TABLE db1.test_table (col1 int, col2 string)')
            # Set a property first so we can remove it
            spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('test.property' = 'test.value')")
        
        # Test fine-grained operations using the test principal
        with IcebergSparkSession(
            credentials=f'{test_principal.principal.client_id}:{test_principal.credentials.client_secret.get_secret_value()}',
            catalog_name=catalog_name,
            polaris_url=polaris_catalog_url,
            use_vended_credentials=False  # Not needed for file storage type
        ) as spark:
            spark.sql(f'USE {catalog_name}')
            
            # UNSET operation should work with TABLE_REMOVE_PROPERTIES
            spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('test.property')")
            
            # SET operation should fail without TABLE_SET_PROPERTIES
            with pytest.raises(Py4JJavaError) as exc_info:
                spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('test.property2' = 'test.value2')")
            
            # Verify the error is related to authorization
            error_str = str(exc_info.value).lower()
            assert "not authorized" in error_str or "forbidden" in error_str, f"Unexpected error: {exc_info.value}"
            
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


def test_multiple_fine_grained_privileges_together(polaris_url, polaris_catalog_url, root_client, fine_grained_authz_test_catalog):
    """Test that multiple fine-grained privileges work together correctly"""
    
    catalog_name = fine_grained_authz_test_catalog.name
    
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
        setup_catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, setup_catalog_role_name)
        
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
        
        # Create test principal with multiple fine-grained privileges
        test_principal = create_principal(polaris_url, polaris_catalog_url, root_client, test_principal_name)
        test_principal_role = create_principal_role(root_client, test_principal_role_name)
        test_catalog_role = create_catalog_role(root_client, fine_grained_authz_test_catalog, test_catalog_role_name)
        
        root_client.assign_catalog_role_to_principal_role(
            principal_role_name=test_principal_role.name,
            catalog_name=catalog_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(catalog_role=test_catalog_role)
        )
        
        # Grant both SET and REMOVE properties privileges
        test_privileges = [
            CatalogPrivilege.TABLE_READ_PROPERTIES,
            CatalogPrivilege.TABLE_SET_PROPERTIES,  # For SET operations
            CatalogPrivilege.TABLE_REMOVE_PROPERTIES  # For UNSET operations
        ]
        
        for privilege in test_privileges:
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
        
        # Test multiple fine-grained operations using the test principal
        with IcebergSparkSession(
            credentials=f'{test_principal.principal.client_id}:{test_principal.credentials.client_secret.get_secret_value()}',
            catalog_name=catalog_name,
            polaris_url=polaris_catalog_url,
            use_vended_credentials=False  # Not needed for file storage type
        ) as spark:
            spark.sql(f'USE {catalog_name}')
            
            # Multiple operations in sequence - all should work with both privileges
            spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('prop1' = 'value1', 'prop2' = 'value2')")
            spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('prop1')")
            spark.sql("ALTER TABLE db1.test_table SET TBLPROPERTIES ('prop3' = 'value3')")
            spark.sql("ALTER TABLE db1.test_table UNSET TBLPROPERTIES ('prop2', 'prop3')")
            
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
