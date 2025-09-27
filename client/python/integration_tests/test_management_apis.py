#
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
#

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
    ResetPrincipalRequest,
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


def test_reset_principal_credentials_default(
    management_client: PolarisDefaultApi,
) -> None:
    principal_name = f"test_principal_for_reset_creds_default"
    principal_with_creds = create_principal(management_client, principal_name)
    initial_client_id = principal_with_creds.principal.client_id
    initial_client_secret = (
        principal_with_creds.credentials.client_secret.get_secret_value()
    )
    try:
        reset_request = ResetPrincipalRequest()
        new_principal_with_creds = management_client.reset_credentials(
            principal_name=principal_name, reset_principal_request=reset_request
        )
        current_client_id = new_principal_with_creds.principal.client_id
        current_client_secret = (
            new_principal_with_creds.credentials.client_secret.get_secret_value()
        )

        assert initial_client_id == current_client_id
        assert initial_client_secret != current_client_secret
    finally:
        management_client.delete_principal(principal_name=principal_name)


def test_reset_principal_credentials_custom(
    management_client: PolarisDefaultApi,
) -> None:
    principal_name = f"test_principal_for_reset_creds_custom"
    create_principal(management_client, principal_name)
    custom_client_id = "e469c048cf866df1"
    custom_client_secret = "1f37adcd21bf1586ed090332eded9cd3"
    try:
        reset_request = ResetPrincipalRequest(
            clientId=custom_client_id, clientSecret=custom_client_secret
        )
        new_principal_with_creds = management_client.reset_credentials(
            principal_name=principal_name, reset_principal_request=reset_request
        )
        current_client_id = new_principal_with_creds.principal.client_id
        current_client_secret = (
            new_principal_with_creds.credentials.client_secret.get_secret_value()
        )

        assert current_client_id == custom_client_id
        assert current_client_secret == custom_client_secret
    finally:
        management_client.delete_principal(principal_name=principal_name)


def test_reset_principal_credentials_custom_client_id(
    management_client: PolarisDefaultApi,
) -> None:
    principal_name = f"test_principal_for_reset_creds_client_id"
    principal_with_creds = create_principal(management_client, principal_name)
    initial_client_secret = (
        principal_with_creds.credentials.client_secret.get_secret_value()
    )
    custom_client_id = "e469c048cf866df1"
    try:
        reset_request = ResetPrincipalRequest(clientId=custom_client_id)
        new_principal_with_creds = management_client.reset_credentials(
            principal_name=principal_name, reset_principal_request=reset_request
        )
        current_client_id = new_principal_with_creds.principal.client_id
        current_client_secret = (
            new_principal_with_creds.credentials.client_secret.get_secret_value()
        )

        assert current_client_id == custom_client_id
        assert initial_client_secret != current_client_secret
    finally:
        management_client.delete_principal(principal_name=principal_name)


def test_reset_principal_credentials_custom_client_secret(
    management_client: PolarisDefaultApi,
) -> None:
    principal_name = f"test_principal_for_reset_creds_client_secret"
    principal_with_creds = create_principal(management_client, principal_name)
    initial_client_id = principal_with_creds.principal.client_id
    custom_client_secret = "1f37adcd21bf1586ed090332eded9cd3"
    try:
        reset_request = ResetPrincipalRequest(clientSecret=custom_client_secret)
        new_principal_with_creds = management_client.reset_credentials(
            principal_name=principal_name, reset_principal_request=reset_request
        )
        current_client_id = new_principal_with_creds.principal.client_id
        current_client_secret = (
            new_principal_with_creds.credentials.client_secret.get_secret_value()
        )

        assert initial_client_id == current_client_id
        assert current_client_secret == custom_client_secret
    finally:
        management_client.delete_principal(principal_name=principal_name)