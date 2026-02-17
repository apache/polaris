/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.admin;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class PolarisAdminServiceAuthzTest extends PolarisAuthzTestBase {
  private PolarisAdminService newTestAdminService() {
    return newTestAdminService(Set.of());
  }

  private PolarisAdminService newTestAdminService(Set<String> activatedPrincipalRoles) {
    final PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);
    return new PolarisAdminService(
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        userSecretsManager,
        serviceIdentityProvider,
        authenticatedPrincipal,
        polarisAuthorizer,
        reservedProperties);
  }

  @TestFactory
  Stream<DynamicNode> testListCatalogsPrivileges() {
    return authzTestsBuilder("listCatalogs")
        .action(() -> newTestAdminService().listCatalogs())
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_LIST)
        .shouldPassWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_CREATE)
        .shouldPassWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateCatalogPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.CATALOG_DROP));
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    final CreateCatalogRequest createRequest =
        new CreateCatalogRequest(newCatalog.asCatalog(serviceIdentityProvider));

    return authzTestsBuilder("createCatalog")
        .action(() -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createCatalog(createRequest))
        .cleanupAction(
            () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).deleteCatalog(newCatalog.getName()))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_CREATE)
        .shouldPassWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetCatalogPrivileges() {
    return authzTestsBuilder("getCatalog")
        .action(() -> newTestAdminService().getCatalog(CATALOG_NAME))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateCatalogPrivileges() {
    return authzTestsBuilder("updateCatalog")
        .action(
            () -> {
              // Use the test-permission admin service instead of the root adminService to also
              // perform the initial GET to illustrate that the actual user workflow for update
              // *must* also encompass GET privileges to be able to set entityVersion properly.
              UpdateCatalogRequest updateRequest =
                  UpdateCatalogRequest.builder()
                      .setCurrentEntityVersion(
                          newTestAdminService().getCatalog(CATALOG_NAME).getEntityVersion())
                      .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                      .build();
              newTestAdminService().updateCatalog(CATALOG_NAME, updateRequest);
            })
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDeleteCatalogPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.CATALOG_CREATE));
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    final CreateCatalogRequest createRequest =
        new CreateCatalogRequest(newCatalog.asCatalog(serviceIdentityProvider));
    adminService.createCatalog(createRequest);

    return authzTestsBuilder("deleteCatalog")
        .action(
            () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).deleteCatalog(newCatalog.getName()))
        .cleanupAction(
            () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createCatalog(createRequest))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_DROP)
        .shouldPassWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_WRITE_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListPrincipalsPrivileges() {
    return authzTestsBuilder("listPrincipals")
        .action(() -> newTestAdminService().listPrincipals())
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_LIST)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_CREATE)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreatePrincipalPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_DROP));
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();

    return authzTestsBuilder("createPrincipal")
        .action(() -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createPrincipal(newPrincipal))
        .cleanupAction(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                    .deletePrincipal(newPrincipal.getName()))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_CREATE)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_DROP)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetPrincipalPrivileges() {
    return authzTestsBuilder("getPrincipal")
        .action(() -> newTestAdminService().getPrincipal(PRINCIPAL_NAME))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_CREATE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdatePrincipalPrivileges() {
    return authzTestsBuilder("updatePrincipal")
        .action(
            () -> {
              // Use the test-permission admin service instead of the root adminService to also
              // perform the initial GET to illustrate that the actual user workflow for update
              // *must* also encompass GET privileges to be able to set entityVersion properly.
              UpdatePrincipalRequest updateRequest =
                  UpdatePrincipalRequest.builder()
                      .setCurrentEntityVersion(
                          newTestAdminService().getPrincipal(PRINCIPAL_NAME).getEntityVersion())
                      .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                      .build();
              newTestAdminService().updatePrincipal(PRINCIPAL_NAME, updateRequest);
            })
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_CREATE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDeletePrincipalPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_CREATE));
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();
    adminService.createPrincipal(newPrincipal);

    return authzTestsBuilder("deletePrincipal")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .deletePrincipal(newPrincipal.getName()))
        .cleanupAction(
            () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createPrincipal(newPrincipal))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_DROP)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_CREATE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListPrincipalRolesPrivileges() {
    return authzTestsBuilder("listPrincipalRoles")
        .action(() -> newTestAdminService().listPrincipalRoles())
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreatePrincipalRolePrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_ROLE_DROP));
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();

    return authzTestsBuilder("createPrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createPrincipalRole(newPrincipalRole))
        .cleanupAction(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                    .deletePrincipalRole(newPrincipalRole.getName()))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetPrincipalRolePrivileges() {
    return authzTestsBuilder("getPrincipalRole")
        .action(() -> newTestAdminService().getPrincipalRole(PRINCIPAL_ROLE2))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdatePrincipalRolePrivileges() {
    return authzTestsBuilder("updatePrincipalRole")
        .action(
            () -> {
              // Use the test-permission admin service instead of the root adminService to also
              // perform the initial GET to illustrate that the actual user workflow for update
              // *must* also encompass GET privileges to be able to set entityVersion properly.
              UpdatePrincipalRoleRequest updateRequest =
                  UpdatePrincipalRoleRequest.builder()
                      .setCurrentEntityVersion(
                          newTestAdminService()
                              .getPrincipalRole(PRINCIPAL_ROLE2)
                              .getEntityVersion())
                      .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                      .build();
              newTestAdminService().updatePrincipalRole(PRINCIPAL_ROLE2, updateRequest);
            })
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDeletePrincipalRolePrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_ROLE_CREATE));
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();
    adminService.createPrincipalRole(newPrincipalRole);

    return authzTestsBuilder("deletePrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .deletePrincipalRole(newPrincipalRole.getName()))
        .cleanupAction(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createPrincipalRole(newPrincipalRole))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_DROP)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListCatalogRolesPrivileges() {
    return authzTestsBuilder("listCatalogRoles")
        .action(() -> newTestAdminService().listCatalogRoles(CATALOG_NAME))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateCatalogRolePrivileges() {
    // Cleanup with CATALOG_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ROLE_DROP));
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();

    return authzTestsBuilder("createCatalogRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .createCatalogRole(CATALOG_NAME, newCatalogRole))
        .cleanupAction(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                    .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGetCatalogRolePrivileges() {
    return authzTestsBuilder("getCatalogRole")
        .action(() -> newTestAdminService().getCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateCatalogRolePrivileges() {
    return authzTestsBuilder("updateCatalogRole")
        .action(
            () -> {
              // Use the test-permission admin service instead of the root adminService to also
              // perform the initial GET to illustrate that the actual user workflow for update
              // *must* also encompass GET privileges to be able to set entityVersion properly.
              UpdateCatalogRoleRequest updateRequest =
                  UpdateCatalogRoleRequest.builder()
                      .setCurrentEntityVersion(
                          newTestAdminService()
                              .getCatalogRole(CATALOG_NAME, CATALOG_ROLE2)
                              .getEntityVersion())
                      .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                      .build();
              newTestAdminService().updateCatalogRole(CATALOG_NAME, CATALOG_ROLE2, updateRequest);
            })
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDeleteCatalogRolePrivileges() {
    // Cleanup with CATALOG_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ROLE_CREATE));
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();
    adminService.createCatalogRole(CATALOG_NAME, newCatalogRole);

    return authzTestsBuilder("deleteCatalogRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()))
        .cleanupAction(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                    .createCatalogRole(CATALOG_NAME, newCatalogRole))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAssignPrincipalRolePrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());

    // Assign only requires privileges on the securable.
    return authzTestsBuilder("assignPrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .assignPrincipalRole("newprincipal", PRINCIPAL_ROLE2))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrincipalRolePrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());

    // Revoke requires privileges both on the "securable" (PrincipalRole) as well as the "grantee"
    // (Principal).
    return authzTestsBuilder("revokePrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokePrincipalRole("newprincipal", PRINCIPAL_ROLE2))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testAssignCatalogRoleToPrincipalRolePrivileges() {
    // Assign only requires privileges on the securable.
    return authzTestsBuilder("assignCatalogRoleToPrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE1))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRevokeCatalogRoleFromPrincipalRolePrivileges() {
    // Revoke requires privileges both on the "securable" (CatalogRole) as well as the "grantee"
    // (PrincipalRole); neither CATALOG_MANAGE_ACCESS nor SERVICE_MANAGE_ACCESS alone are
    // sufficient.
    return authzTestsBuilder("revokeCatalogRoleFromPrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokeCatalogRoleFromPrincipalRole(
                        PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE1))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS, PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldPassWith(
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListAssigneePrincipalRolesForCatalogRolePrivileges() {
    return authzTestsBuilder("listAssigneePrincipalRolesForCatalogRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .listAssigneePrincipalRolesForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnCatalogFromRole(
                    CATALOG_NAME, CATALOG_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListGrantsForCatalogRolePrivileges() {
    return authzTestsBuilder("listGrantsForCatalogRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnCatalogFromRole(
                    CATALOG_NAME, CATALOG_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldPassWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_CREATE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_DROP)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnRootContainerToPrincipalRolePrivileges() {
    return authzTestsBuilder("grantPrivilegeOnRootContainerToPrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .grantPrivilegeOnRootContainerToPrincipalRole(
                        PRINCIPAL_ROLE2, PolarisPrivilege.SERVICE_MANAGE_ACCESS))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnRootContainerFromPrincipalRolePrivileges() {
    return authzTestsBuilder("revokePrivilegeOnRootContainerFromPrincipalRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokePrivilegeOnRootContainerFromPrincipalRole(
                        PRINCIPAL_ROLE2, PolarisPrivilege.SERVICE_MANAGE_ACCESS))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnCatalogToRolePrivileges() {
    return authzTestsBuilder("grantPrivilegeOnCatalogToRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .grantPrivilegeOnCatalogToRole(
                        CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnCatalogFromRolePrivileges() {
    return authzTestsBuilder("revokePrivilegeOnCatalogFromRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokePrivilegeOnCatalogFromRole(
                        CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnNamespaceToRolePrivileges() {
    return Stream.of(CATALOG_NAME, FEDERATED_CATALOG_NAME)
        .flatMap(
            catalogName ->
                authzTestsBuilder("grantPrivilegeOnNamespaceToRole[" + catalogName + "]")
                    .action(
                        () ->
                            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                                .grantPrivilegeOnNamespaceToRole(
                                    catalogName,
                                    CATALOG_ROLE2,
                                    NS1,
                                    PolarisPrivilege.CATALOG_MANAGE_ACCESS))
                    .grantAction(
                        privilege ->
                            adminService.grantPrivilegeOnCatalogToRole(
                                catalogName, CATALOG_ROLE1, privilege))
                    .revokeAction(
                        privilege ->
                            adminService.revokePrivilegeOnCatalogFromRole(
                                catalogName, CATALOG_ROLE1, privilege))
                    .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
                    .shouldPassWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
                    .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
                    .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
                    .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
                    .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
                    .createTests());
  }

  @TestFactory
  Stream<DynamicNode>
      testGrantPrivilegeOnNamespaceToRoleSufficientPrivileges_FederationNestedNamespace() {
    return authzTestsBuilder("grantPrivilegeOnNamespaceToRole_FederationNestedNamespace")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .grantPrivilegeOnNamespaceToRole(
                        FEDERATED_CATALOG_NAME,
                        CATALOG_ROLE2,
                        NS1AA,
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .grantAction(
            privilege ->
                adminService.grantPrivilegeOnCatalogToRole(
                    FEDERATED_CATALOG_NAME, CATALOG_ROLE1, privilege))
        .revokeAction(
            privilege ->
                adminService.revokePrivilegeOnCatalogFromRole(
                    FEDERATED_CATALOG_NAME, CATALOG_ROLE1, privilege))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnNamespaceFromRolePrivileges() {
    return authzTestsBuilder("revokePrivilegeOnNamespaceFromRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokePrivilegeOnNamespaceFromRole(
                        CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnTableToRolePrivileges() {
    return Stream.of(CATALOG_NAME, FEDERATED_CATALOG_NAME)
        .flatMap(
            catalogName ->
                authzTestsBuilder("grantPrivilegeOnTableToRole[" + catalogName + "]")
                    .action(
                        () ->
                            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                                .grantPrivilegeOnTableToRole(
                                    catalogName,
                                    CATALOG_ROLE2,
                                    TABLE_NS1_1,
                                    PolarisPrivilege.CATALOG_MANAGE_ACCESS))
                    .grantAction(
                        privilege ->
                            adminService.grantPrivilegeOnCatalogToRole(
                                catalogName, CATALOG_ROLE1, privilege))
                    .revokeAction(
                        privilege ->
                            adminService.revokePrivilegeOnCatalogFromRole(
                                catalogName, CATALOG_ROLE1, privilege))
                    .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
                    .shouldPassWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
                    .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
                    .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
                    .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
                    .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
                    .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
                    .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
                    .createTests());
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnTableFromRolePrivileges() {
    return authzTestsBuilder("revokePrivilegeOnTableFromRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokePrivilegeOnTableFromRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        TABLE_NS1_1,
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnViewToRolePrivileges() {
    return authzTestsBuilder("grantPrivilegeOnViewToRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .grantPrivilegeOnViewToRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        VIEW_NS1_1,
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnViewFromRolePrivileges() {
    return authzTestsBuilder("revokePrivilegeOnViewFromRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .revokePrivilegeOnViewFromRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        VIEW_NS1_1,
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnPolicyToRolePrivileges() {
    return authzTestsBuilder("grantPrivilegeOnPolicyToRole")
        .action(
            () ->
                newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                    .grantPrivilegeOnPolicyToRole(
                        CATALOG_NAME,
                        CATALOG_ROLE2,
                        POLICY_NS1_1,
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_ACCESS)
        .shouldPassWith(PolarisPrivilege.POLICY_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.CATALOG_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.NAMESPACE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.TABLE_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE)
        .shouldFailWith(PolarisPrivilege.VIEW_LIST_GRANTS)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_FULL_METADATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.SERVICE_MANAGE_ACCESS)
        .createTests();
  }
}
