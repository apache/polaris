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
package io.polaris.service.admin;

import io.polaris.core.admin.model.UpdateCatalogRequest;
import io.polaris.core.admin.model.UpdateCatalogRoleRequest;
import io.polaris.core.admin.model.UpdatePrincipalRequest;
import io.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import io.polaris.core.auth.AuthenticatedPolarisPrincipal;
import io.polaris.core.entity.CatalogEntity;
import io.polaris.core.entity.CatalogRoleEntity;
import io.polaris.core.entity.PolarisPrivilege;
import io.polaris.core.entity.PrincipalEntity;
import io.polaris.core.entity.PrincipalRoleEntity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class PolarisAdminServiceAuthzTest extends PolarisAuthzTestBase {
  private PolarisAdminService newTestAdminService() {
    return newTestAdminService(Set.of());
  }

  private PolarisAdminService newTestAdminService(Set<String> activatedPrincipalRoles) {
    final AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipal(principalEntity, activatedPrincipalRoles);
    return new PolarisAdminService(
        callContext, entityManager, authenticatedPrincipal, polarisAuthorizer);
  }

  private void doTestSufficientPrivileges(
      List<PolarisPrivilege> sufficientPrivileges,
      Runnable action,
      Runnable cleanupAction,
      Function<PolarisPrivilege, Boolean> grantAction,
      Function<PolarisPrivilege, Boolean> revokeAction) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges.stream().map(priv -> Set.of(priv)).toList(),
        action,
        cleanupAction,
        PRINCIPAL_NAME,
        grantAction,
        revokeAction);
  }

  private void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges,
      Runnable action,
      Function<PolarisPrivilege, Boolean> grantAction,
      Function<PolarisPrivilege, Boolean> revokeAction) {
    doTestInsufficientPrivileges(
        insufficientPrivileges, PRINCIPAL_NAME, action, grantAction, revokeAction);
  }

  @Test
  public void testListCatalogsSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_LIST,
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_CREATE,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newTestAdminService().listCatalogs(),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testListCatalogsInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () -> newTestAdminService().listCatalogs(),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testCreateCatalogSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                PRINCIPAL_ROLE2, PolarisPrivilege.CATALOG_DROP))
        .isTrue();
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_CREATE,
            PolarisPrivilege.CATALOG_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createCatalog(newCatalog),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).deleteCatalog(newCatalog.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testCreateCatalogInsufficientPrivileges() {
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_LIST,
            PolarisPrivilege.CATALOG_DROP,
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createCatalog(newCatalog),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGetCatalogSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newTestAdminService().getCatalog(CATALOG_NAME),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGetCatalogInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_LIST,
            PolarisPrivilege.CATALOG_CREATE,
            PolarisPrivilege.CATALOG_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () -> newTestAdminService().getCatalog(CATALOG_NAME),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdateCatalogSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
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
        },
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdateCatalogInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_LIST,
            PolarisPrivilege.CATALOG_CREATE,
            PolarisPrivilege.CATALOG_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () -> {
          UpdateCatalogRequest updateRequest =
              UpdateCatalogRequest.builder()
                  .setCurrentEntityVersion(
                      newTestAdminService().getCatalog(CATALOG_NAME).getEntityVersion())
                  .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                  .build();
          newTestAdminService().updateCatalog(CATALOG_NAME, updateRequest);
        },
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testDeleteCatalogSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                PRINCIPAL_ROLE2, PolarisPrivilege.CATALOG_CREATE))
        .isTrue();
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    adminService.createCatalog(newCatalog);

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_DROP,
            PolarisPrivilege.CATALOG_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).deleteCatalog(newCatalog.getName()),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createCatalog(newCatalog),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testDeleteCatalogInsufficientPrivileges() {
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    adminService.createCatalog(newCatalog);

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_CREATE,
            PolarisPrivilege.CATALOG_LIST,
            PolarisPrivilege.CATALOG_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).deleteCatalog(newCatalog.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testListPrincipalsSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_LIST,
            PolarisPrivilege.PRINCIPAL_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_CREATE,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA),
        () -> newTestAdminService().listPrincipals(),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testListPrincipalsInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().listPrincipals(),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testCreatePrincipalSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_DROP))
        .isTrue();
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_CREATE,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createPrincipal(newPrincipal),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).deletePrincipal(newPrincipal.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testCreatePrincipalInsufficientPrivileges() {
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_LIST,
            PolarisPrivilege.PRINCIPAL_DROP,
            PolarisPrivilege.PRINCIPAL_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createPrincipal(newPrincipal),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGetPrincipalSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA),
        () -> newTestAdminService().getPrincipal(PRINCIPAL_NAME),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGetPrincipalInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_LIST,
            PolarisPrivilege.PRINCIPAL_CREATE,
            PolarisPrivilege.PRINCIPAL_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().getPrincipal(PRINCIPAL_NAME),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdatePrincipalSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA),
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
        },
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdatePrincipalInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_LIST,
            PolarisPrivilege.PRINCIPAL_CREATE,
            PolarisPrivilege.PRINCIPAL_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> {
          UpdatePrincipalRequest updateRequest =
              UpdatePrincipalRequest.builder()
                  .setCurrentEntityVersion(
                      newTestAdminService().getPrincipal(PRINCIPAL_NAME).getEntityVersion())
                  .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                  .build();
          newTestAdminService().updatePrincipal(PRINCIPAL_NAME, updateRequest);
        },
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testDeletePrincipalSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_CREATE))
        .isTrue();
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();
    adminService.createPrincipal(newPrincipal);

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_DROP,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).deletePrincipal(newPrincipal.getName()),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createPrincipal(newPrincipal),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testDeletePrincipalInsufficientPrivileges() {
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();
    adminService.createPrincipal(newPrincipal);

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_CREATE,
            PolarisPrivilege.PRINCIPAL_LIST,
            PolarisPrivilege.PRINCIPAL_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_WRITE_PROPERTIES),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).deletePrincipal(newPrincipal.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testListPrincipalRolesSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST,
            PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_CREATE,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> newTestAdminService().listPrincipalRoles(),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testListPrincipalRolesInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().listPrincipalRoles(),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testCreatePrincipalRoleSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_ROLE_DROP))
        .isTrue();
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_CREATE,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createPrincipalRole(newPrincipalRole),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                .deletePrincipalRole(newPrincipalRole.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testCreatePrincipalRoleInsufficientPrivileges() {
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST,
            PolarisPrivilege.PRINCIPAL_ROLE_DROP,
            PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createPrincipalRole(newPrincipalRole),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGetPrincipalRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> newTestAdminService().getPrincipalRole(PRINCIPAL_ROLE2),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGetPrincipalRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST,
            PolarisPrivilege.PRINCIPAL_ROLE_CREATE,
            PolarisPrivilege.PRINCIPAL_ROLE_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().getPrincipalRole(PRINCIPAL_ROLE2),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdatePrincipalRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> {
          // Use the test-permission admin service instead of the root adminService to also
          // perform the initial GET to illustrate that the actual user workflow for update
          // *must* also encompass GET privileges to be able to set entityVersion properly.
          UpdatePrincipalRoleRequest updateRequest =
              UpdatePrincipalRoleRequest.builder()
                  .setCurrentEntityVersion(
                      newTestAdminService().getPrincipalRole(PRINCIPAL_ROLE2).getEntityVersion())
                  .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                  .build();
          newTestAdminService().updatePrincipalRole(PRINCIPAL_ROLE2, updateRequest);
        },
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdatePrincipalRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST,
            PolarisPrivilege.PRINCIPAL_ROLE_CREATE,
            PolarisPrivilege.PRINCIPAL_ROLE_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> {
          UpdatePrincipalRoleRequest updateRequest =
              UpdatePrincipalRoleRequest.builder()
                  .setCurrentEntityVersion(
                      newTestAdminService().getPrincipalRole(PRINCIPAL_ROLE2).getEntityVersion())
                  .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                  .build();
          newTestAdminService().updatePrincipalRole(PRINCIPAL_ROLE2, updateRequest);
        },
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testDeletePrincipalRoleSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(
                PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_ROLE_CREATE))
        .isTrue();
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();
    adminService.createPrincipalRole(newPrincipalRole);

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_DROP,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .deletePrincipalRole(newPrincipalRole.getName()),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createPrincipalRole(newPrincipalRole),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testDeletePrincipalRoleInsufficientPrivileges() {
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();
    adminService.createPrincipalRole(newPrincipalRole);

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_CREATE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST,
            PolarisPrivilege.PRINCIPAL_ROLE_READ_PROPERTIES,
            PolarisPrivilege.PRINCIPAL_ROLE_WRITE_PROPERTIES),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .deletePrincipalRole(newPrincipalRole.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testListCatalogRolesSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().listCatalogRoles(CATALOG_NAME),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testListCatalogRolesInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> newTestAdminService().listCatalogRoles(CATALOG_NAME),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testCreateCatalogRoleSufficientPrivileges() {
    // Cleanup with CATALOG_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ROLE_DROP))
        .isTrue();
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .createCatalogRole(CATALOG_NAME, newCatalogRole),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testCreateCatalogRoleInsufficientPrivileges() {
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .createCatalogRole(CATALOG_NAME, newCatalogRole),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGetCatalogRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().getCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGetCatalogRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> newTestAdminService().getCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testUpdateCatalogRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
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
        },
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testUpdateCatalogRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA),
        () -> {
          UpdateCatalogRoleRequest updateRequest =
              UpdateCatalogRoleRequest.builder()
                  .setCurrentEntityVersion(
                      newTestAdminService()
                          .getCatalogRole(CATALOG_NAME, CATALOG_ROLE2)
                          .getEntityVersion())
                  .setProperties(Map.of("foo", Long.toString(System.currentTimeMillis())))
                  .build();
          newTestAdminService().updateCatalogRole(CATALOG_NAME, CATALOG_ROLE2, updateRequest);
        },
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testDeleteCatalogRoleSufficientPrivileges() {
    // Cleanup with CATALOG_ROLE2
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ROLE_CREATE))
        .isTrue();
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();
    adminService.createCatalogRole(CATALOG_NAME, newCatalogRole);

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                .createCatalogRole(CATALOG_NAME, newCatalogRole),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testDeleteCatalogRoleInsufficientPrivileges() {
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();
    adminService.createCatalogRole(CATALOG_NAME, newCatalogRole);

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }
}
