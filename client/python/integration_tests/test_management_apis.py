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
from integration_tests.conftest import (
    create_principal,
    create_principal_role,
    create_catalog_role,
)
from polaris.management import (
    GrantPrincipalRoleRequest,
    GrantCatalogRoleRequest,
    AddGrantRequest,
    CatalogGrant,
    CatalogPrivilege,
    RevokeGrantRequest,
    PolarisDefaultApi,
    Catalog,
)


def test_principals(management_client: PolarisDefaultApi) -> None:
    principal_name = "test_principal_2"
    create_principal(management_client, principal_name)
    try:
        assert len(management_client.list_principals().principals) > 0
        principal = management_client.get_principal(principal_name)
        assert principal.name == principal_name
    finally:
        management_client.delete_principal(principal_name)


def test_principal_and_principal_roles(management_client: PolarisDefaultApi) -> None:
    principal_name = "test_principal_2"
    principal_role_name = "test_role_2"
    create_principal(management_client, principal_name)
    principal_role = create_principal_role(management_client, principal_role_name)
    try:
        management_client.assign_principal_role(
            principal_name,
            grant_principal_role_request=GrantPrincipalRoleRequest(
                principal_role=principal_role
            ),
        )
        result_principals = (
            management_client.list_assignee_principals_for_principal_role(
                principal_role_name
            ).principals
        )
        assert len(result_principals) == 1
        assert result_principals[0].name == principal_name
    finally:
        management_client.delete_principal(principal_name)
        management_client.delete_principal_role(principal_role_name)


def test_catalog_roles(
    management_client: PolarisDefaultApi, test_catalog: Catalog
) -> None:
    catalog_role = create_catalog_role(management_client, test_catalog, "test_role_1")
    principal_role_name = "test_role_2"
    create_principal_role(management_client, principal_role_name)
    try:
        management_client.assign_catalog_role_to_principal_role(
            principal_role_name=principal_role_name,
            catalog_name=test_catalog.name,
            grant_catalog_role_request=GrantCatalogRoleRequest(
                catalog_role=catalog_role
            ),
        )
        principal_roles = (
            management_client.list_assignee_principal_roles_for_catalog_role(
                test_catalog.name, catalog_role.name
            )
        )
        assert len(principal_roles.roles) == 1
        assert principal_roles.roles[0].name == principal_role_name
    finally:
        management_client.delete_catalog_role(test_catalog.name, catalog_role.name)
        management_client.delete_principal_role(principal_role_name)


def test_grants(management_client: PolarisDefaultApi, test_catalog: Catalog) -> None:
    catalog_role = create_catalog_role(management_client, test_catalog, "test_role_1")
    try:
        management_client.add_grant_to_catalog_role(
            test_catalog.name,
            catalog_role.name,
            AddGrantRequest(
                grant=CatalogGrant(
                    type="catalog", privilege=CatalogPrivilege.CATALOG_MANAGE_CONTENT
                )
            ),
        )
        grants = management_client.list_grants_for_catalog_role(
            test_catalog.name, catalog_role.name
        )
        assert len(grants.grants) == 1
        assert grants.grants[0].privilege == CatalogPrivilege.CATALOG_MANAGE_CONTENT
        management_client.revoke_grant_from_catalog_role(
            test_catalog.name,
            catalog_role.name,
            False,
            RevokeGrantRequest(
                grant=CatalogGrant(
                    type="catalog", privilege=CatalogPrivilege.CATALOG_MANAGE_CONTENT
                )
            ),
        )
        grants = management_client.list_grants_for_catalog_role(
            test_catalog.name, catalog_role.name
        )
        assert len(grants.grants) == 0
    finally:
        management_client.delete_catalog_role(test_catalog.name, catalog_role.name)
