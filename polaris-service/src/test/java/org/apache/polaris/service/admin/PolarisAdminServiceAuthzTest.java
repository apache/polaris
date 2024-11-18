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

import static org.apache.polaris.core.entity.PolarisEntityConstants.MAINTENANCE_PREFIX;
import static org.apache.polaris.core.entity.PolarisMaintenanceProperties.COMPACTION;
import static org.apache.polaris.service.admin.PolarisAdminService.maintenancePropertyChanged;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class PolarisAdminServiceAuthzTest extends PolarisAuthzTestBase {
  private PolarisAdminService newTestAdminService() {
    return newTestAdminService(Set.of());
  }

  private PolarisAdminService newTestAdminService(Set<String> activatedPrincipalRoles) {
    final AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipal(principalEntity, activatedPrincipalRoles);
    return new PolarisAdminService(
        callContext, entityManager, metaStoreManager, authenticatedPrincipal, polarisAuthorizer);
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
            PolarisPrivilege.CATALOG_WRITE_MAINTENANCE_PROPERTIES,
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
  public void testUpdateCatalogSufficientPrivilegesForMaintenanceProperties() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_WRITE_MAINTENANCE_PROPERTIES,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        updateCatalogMaintenanceProperty(),
        null,
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testUpdateCatalogInsufficientPrivilegesForMaintenanceProperties() {
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
            PolarisPrivilege.CATALOG_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        updateCatalogMaintenanceProperty(),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  private @NotNull Runnable updateCatalogMaintenanceProperty() {
    return () -> {
      var entityVersion = newTestAdminService().getCatalog(CATALOG_NAME).getEntityVersion();
      var updateRequest =
          UpdateCatalogRequest.builder()
              .setCurrentEntityVersion(entityVersion)
              .setProperties(Map.of(COMPACTION.getValue(), "{}"))
              .build();
      newTestAdminService().updateCatalog(CATALOG_NAME, updateRequest);
    };
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

  @Test
  public void testAssignPrincipalRoleSufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());

    // Assign only requires privileges on the securable.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .assignPrincipalRole("newprincipal", PRINCIPAL_ROLE2),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testAssignPrincipalRoleInsufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .assignPrincipalRole("newprincipal", PRINCIPAL_ROLE2),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testRevokePrincipalRoleSufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());

    // Revoke requires privileges both on the "securable" (PrincipalRole) as well as the "grantee"
    // (Principal).
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.SERVICE_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrincipalRole("newprincipal", PRINCIPAL_ROLE2),
        null, // cleanupAction
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testRevokePrincipalRoleInsufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrincipalRole("newprincipal", PRINCIPAL_ROLE2),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testAssignCatalogRoleToPrincipalRoleSufficientPrivileges() {
    // Assign only requires privileges on the securable.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE1),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testAssignCatalogRoleToPrincipalRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE1),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testRevokeCatalogRoleFromPrincipalRoleSufficientPrivileges() {
    // Revoke requires privileges both on the "securable" (CatalogRole) as well as the "grantee"
    // (PrincipalRole); neither CATALOG_MANAGE_ACCESS nor SERVICE_MANAGE_ACCESS alone are
    // sufficient.
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS, PolarisPrivilege.SERVICE_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.CATALOG_MANAGE_ACCESS,
                PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE),
            Set.of(
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokeCatalogRoleFromPrincipalRole(PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE1),
        null, // cleanupAction
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testRevokeCatalogRoleFromPrincipalRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokeCatalogRoleFromPrincipalRole(PRINCIPAL_ROLE2, CATALOG_NAME, CATALOG_ROLE1),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnRootContainerToPrincipalRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE2, PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnRootContainerToPrincipalRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnRootContainerToPrincipalRole(
                    PRINCIPAL_ROLE2, PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnRootContainerFromPrincipalRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE2, PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnRootContainerFromPrincipalRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnRootContainerFromPrincipalRole(
                    PRINCIPAL_ROLE2, PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnCatalogToRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnCatalogToRole(
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnCatalogToRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnCatalogToRole(
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnCatalogFromRoleSufficientPrivileges() {
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnCatalogFromRole(
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnCatalogFromRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnCatalogFromRole(
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnNamespaceToRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnNamespaceToRole(
                    CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnNamespaceToRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnNamespaceToRole(
                    CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnNamespaceFromRoleSufficientPrivileges() {
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnNamespaceFromRole(
                    CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnNamespaceFromRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnNamespaceFromRole(
                    CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnTableToRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnTableToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TABLE_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnTableToRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnTableToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TABLE_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnTableFromRoleSufficientPrivileges() {
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnTableFromRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TABLE_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnTableFromRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnTableFromRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    TABLE_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnViewToRoleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnViewToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    VIEW_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testGrantPrivilegeOnViewToRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnViewToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    VIEW_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnViewFromRoleSufficientPrivileges() {
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnViewFromRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    VIEW_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testRevokePrivilegeOnViewFromRoleInsufficientPrivileges() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.PRINCIPAL_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.NAMESPACE_LIST_GRANTS,
            PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.TABLE_LIST_GRANTS,
            PolarisPrivilege.VIEW_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.VIEW_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT,
            PolarisPrivilege.SERVICE_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnViewFromRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    VIEW_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testMaintenancePropertyChanged() {
    // Scenario 1: Added Maintenance Key
    Map<String, String> map1 = new HashMap<>();
    Map<String, String> map2 = new HashMap<>();
    map2.put(MAINTENANCE_PREFIX + "key1", "value1");

    assertTrue(
        maintenancePropertyChanged(map1, map2), "Should return true for added maintenance key");

    // Scenario 2: Removed Maintenance Key
    map1.put(MAINTENANCE_PREFIX + "key1", "value1");
    map2.clear();

    assertTrue(
        maintenancePropertyChanged(map1, map2), "Should return true for removed maintenance key");

    // Scenario 3: Changed Maintenance Key
    map1.put(MAINTENANCE_PREFIX + "key1", "value1");
    map2.put(MAINTENANCE_PREFIX + "key1", "value2");

    assertTrue(
        maintenancePropertyChanged(map1, map2), "Should return true for changed maintenance key");

    // Scenario 4: No Maintenance Keys Involved
    map1.clear();
    map2.clear();
    map1.put("other.key1", "value1");
    map2.put("other.key1", "value1");

    assertFalse(
        maintenancePropertyChanged(map1, map2),
        "Should return false when no maintenance keys are involved");

    // Scenario 5: Added Non-Maintenance Key
    map1.clear();
    map2.clear();
    map2.put("other.key1", "value1");

    assertFalse(
        maintenancePropertyChanged(map1, map2),
        "Should return false for added non-maintenance key");
  }
}
