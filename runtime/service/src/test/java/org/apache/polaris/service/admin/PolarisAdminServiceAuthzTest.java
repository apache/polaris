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
import java.util.List;
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
import org.junit.jupiter.api.DynamicTest;
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
  Stream<DynamicNode> testListCatalogsSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listCatalogs",
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

  @TestFactory
  Stream<DynamicTest> testListCatalogsInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listCatalogs",
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

  @TestFactory
  Stream<DynamicNode> testCreateCatalogSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.CATALOG_DROP));
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    final CreateCatalogRequest createRequest =
        new CreateCatalogRequest(newCatalog.asCatalog(serviceIdentityProvider));

    return doTestSufficientPrivileges(
        "createCatalog",
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_CREATE,
            PolarisPrivilege.CATALOG_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createCatalog(createRequest),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).deleteCatalog(newCatalog.getName()),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testCreateCatalogInsufficientPrivileges() {
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    final CreateCatalogRequest createRequest =
        new CreateCatalogRequest(newCatalog.asCatalog(serviceIdentityProvider));

    return doTestInsufficientPrivileges(
        "createCatalog",
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
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).createCatalog(createRequest),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicNode> testGetCatalogSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getCatalog",
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

  @TestFactory
  Stream<DynamicTest> testGetCatalogInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getCatalog",
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

  @TestFactory
  Stream<DynamicNode> testUpdateCatalogSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "updateCatalog",
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

  @TestFactory
  Stream<DynamicTest> testUpdateCatalogInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "updateCatalog",
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

  @TestFactory
  Stream<DynamicNode> testDeleteCatalogSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.CATALOG_CREATE));
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    final CreateCatalogRequest createRequest =
        new CreateCatalogRequest(newCatalog.asCatalog(serviceIdentityProvider));
    adminService.createCatalog(createRequest);

    return doTestSufficientPrivileges(
        "deleteCatalog",
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_DROP,
            PolarisPrivilege.CATALOG_FULL_METADATA),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE1)).deleteCatalog(newCatalog.getName()),
        () -> newTestAdminService(Set.of(PRINCIPAL_ROLE2)).createCatalog(createRequest),
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testDeleteCatalogInsufficientPrivileges() {
    final CatalogEntity newCatalog = new CatalogEntity.Builder().setName("new_catalog").build();
    final CreateCatalogRequest createRequest =
        new CreateCatalogRequest(newCatalog.asCatalog(serviceIdentityProvider));
    adminService.createCatalog(createRequest);

    return doTestInsufficientPrivileges(
        "deleteCatalog",
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

  @TestFactory
  Stream<DynamicNode> testListPrincipalsSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listPrincipals",
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

  @TestFactory
  Stream<DynamicTest> testListPrincipalsInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listPrincipals",
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

  @TestFactory
  Stream<DynamicNode> testCreatePrincipalSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_DROP));
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();

    return doTestSufficientPrivileges(
        "createPrincipal",
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

  @TestFactory
  Stream<DynamicTest> testCreatePrincipalInsufficientPrivileges() {
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();

    return doTestInsufficientPrivileges(
        "createPrincipal",
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

  @TestFactory
  Stream<DynamicNode> testGetPrincipalSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getPrincipal",
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

  @TestFactory
  Stream<DynamicTest> testGetPrincipalInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getPrincipal",
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

  @TestFactory
  Stream<DynamicNode> testUpdatePrincipalSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "updatePrincipal",
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

  @TestFactory
  Stream<DynamicTest> testUpdatePrincipalInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "updatePrincipal",
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

  @TestFactory
  Stream<DynamicNode> testDeletePrincipalSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_CREATE));
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();
    adminService.createPrincipal(newPrincipal);

    return doTestSufficientPrivileges(
        "deletePrincipal",
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

  @TestFactory
  Stream<DynamicTest> testDeletePrincipalInsufficientPrivileges() {
    final PrincipalEntity newPrincipal =
        new PrincipalEntity.Builder().setName("new_principal").build();
    adminService.createPrincipal(newPrincipal);

    return doTestInsufficientPrivileges(
        "deletePrincipal",
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

  @TestFactory
  Stream<DynamicNode> testListPrincipalRolesSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listPrincipalRoles",
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

  @TestFactory
  Stream<DynamicTest> testListPrincipalRolesInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listPrincipalRoles",
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

  @TestFactory
  Stream<DynamicNode> testCreatePrincipalRoleSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_ROLE_DROP));
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();

    return doTestSufficientPrivileges(
        "createPrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testCreatePrincipalRoleInsufficientPrivileges() {
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();

    return doTestInsufficientPrivileges(
        "createPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testGetPrincipalRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getPrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testGetPrincipalRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testUpdatePrincipalRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "updatePrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testUpdatePrincipalRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "updatePrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testDeletePrincipalRoleSufficientPrivileges() {
    // Cleanup with PRINCIPAL_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnRootContainerToPrincipalRole(
            PRINCIPAL_ROLE2, PolarisPrivilege.PRINCIPAL_ROLE_CREATE));
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();
    adminService.createPrincipalRole(newPrincipalRole);

    return doTestSufficientPrivileges(
        "deletePrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testDeletePrincipalRoleInsufficientPrivileges() {
    final PrincipalRoleEntity newPrincipalRole =
        new PrincipalRoleEntity.Builder().setName("new_principal_role").build();
    adminService.createPrincipalRole(newPrincipalRole);

    return doTestInsufficientPrivileges(
        "deletePrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testListCatalogRolesSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listCatalogRoles",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().listCatalogRoles(CATALOG_NAME),
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testListCatalogRolesInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listCatalogRoles",
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
        () -> newTestAdminService().listCatalogRoles(CATALOG_NAME));
  }

  @TestFactory
  Stream<DynamicNode> testCreateCatalogRoleSufficientPrivileges() {
    // Cleanup with CATALOG_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ROLE_DROP));
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();

    return doTestSufficientPrivileges(
        "createCatalogRole",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .createCatalogRole(CATALOG_NAME, newCatalogRole),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()));
  }

  @TestFactory
  Stream<DynamicTest> testCreateCatalogRoleInsufficientPrivileges() {
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();

    return doTestInsufficientPrivileges(
        "createCatalogRole",
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
                .createCatalogRole(CATALOG_NAME, newCatalogRole));
  }

  @TestFactory
  Stream<DynamicNode> testGetCatalogRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "getCatalogRole",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () -> newTestAdminService().getCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testGetCatalogRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "getCatalogRole",
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
        () -> newTestAdminService().getCatalogRole(CATALOG_NAME, CATALOG_ROLE2));
  }

  @TestFactory
  Stream<DynamicNode> testUpdateCatalogRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "updateCatalogRole",
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
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testUpdateCatalogRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "updateCatalogRole",
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
        });
  }

  @TestFactory
  Stream<DynamicNode> testDeleteCatalogRoleSufficientPrivileges() {
    // Cleanup with CATALOG_ROLE2
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_ROLE_CREATE));
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();
    adminService.createCatalogRole(CATALOG_NAME, newCatalogRole);

    return doTestSufficientPrivileges(
        "deleteCatalogRole",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.CATALOG_ROLE_FULL_METADATA),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE2))
                .createCatalogRole(CATALOG_NAME, newCatalogRole));
  }

  @TestFactory
  Stream<DynamicTest> testDeleteCatalogRoleInsufficientPrivileges() {
    final CatalogRoleEntity newCatalogRole =
        new CatalogRoleEntity.Builder().setName("new_catalog_role").build();
    adminService.createCatalogRole(CATALOG_NAME, newCatalogRole);

    return doTestInsufficientPrivileges(
        "deleteCatalogRole",
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
                .deleteCatalogRole(CATALOG_NAME, newCatalogRole.getName()));
  }

  @TestFactory
  Stream<DynamicNode> testAssignPrincipalRoleSufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());

    // Assign only requires privileges on the securable.
    return doTestSufficientPrivileges(
        "assignPrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testAssignPrincipalRoleInsufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());
    return doTestInsufficientPrivileges(
        "assignPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testRevokePrincipalRoleSufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());

    // Revoke requires privileges both on the "securable" (PrincipalRole) as well as the "grantee"
    // (Principal).
    return doTestSufficientPrivilegeSets(
        "revokePrincipalRole",
        List.of(
            Set.of(PolarisPrivilege.SERVICE_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.PRINCIPAL_ROLE_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.PRINCIPAL_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrincipalRole("newprincipal", PRINCIPAL_ROLE2),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testRevokePrincipalRoleInsufficientPrivileges() {
    adminService.createPrincipal(new PrincipalEntity.Builder().setName("newprincipal").build());
    return doTestInsufficientPrivileges(
        "revokePrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testAssignCatalogRoleToPrincipalRoleSufficientPrivileges() {
    // Assign only requires privileges on the securable.
    return doTestSufficientPrivileges(
        "assignCatalogRoleToPrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testAssignCatalogRoleToPrincipalRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "assignCatalogRoleToPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testRevokeCatalogRoleFromPrincipalRoleSufficientPrivileges() {
    // Revoke requires privileges both on the "securable" (CatalogRole) as well as the "grantee"
    // (PrincipalRole); neither CATALOG_MANAGE_ACCESS nor SERVICE_MANAGE_ACCESS alone are
    // sufficient.
    return doTestSufficientPrivilegeSets(
        "revokeCatalogRoleFromPrincipalRole",
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
        (privilege) ->
            adminService.grantPrivilegeOnRootContainerToPrincipalRole(PRINCIPAL_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnRootContainerFromPrincipalRole(
                PRINCIPAL_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testRevokeCatalogRoleFromPrincipalRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "revokeCatalogRoleFromPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testListAssigneePrincipalRolesForCatalogRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listAssigneePrincipalRolesForCatalogRole",
        List.of(
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .listAssigneePrincipalRolesForCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        null, // cleanupAction
        privilege ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        privilege ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testListAssigneePrincipalRolesForCatalogRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listAssigneePrincipalRolesForCatalogRole",
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .listAssigneePrincipalRolesForCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        privilege ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        privilege ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicNode> testListGrantsForCatalogRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listGrantsForCatalogRole",
        List.of(
            PolarisPrivilege.CATALOG_ROLE_LIST_GRANTS,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_ON_SECURABLE,
            PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE,
            PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        null, // cleanupAction
        privilege ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        privilege ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testListGrantsForCatalogRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listGrantsForCatalogRole",
        List.of(
            PolarisPrivilege.SERVICE_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_ROLE_LIST,
            PolarisPrivilege.CATALOG_ROLE_READ_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_WRITE_PROPERTIES,
            PolarisPrivilege.CATALOG_ROLE_CREATE,
            PolarisPrivilege.CATALOG_ROLE_DROP,
            PolarisPrivilege.CATALOG_LIST_GRANTS,
            PolarisPrivilege.PRINCIPAL_FULL_METADATA,
            PolarisPrivilege.PRINCIPAL_ROLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .listGrantsForCatalogRole(CATALOG_NAME, CATALOG_ROLE2),
        privilege ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        privilege ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnRootContainerToPrincipalRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "grantPrivilegeOnRootContainerToPrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testGrantPrivilegeOnRootContainerToPrincipalRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "grantPrivilegeOnRootContainerToPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnRootContainerFromPrincipalRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "revokePrivilegeOnRootContainerFromPrincipalRole",
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

  @TestFactory
  Stream<DynamicTest> testRevokePrivilegeOnRootContainerFromPrincipalRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "revokePrivilegeOnRootContainerFromPrincipalRole",
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

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnCatalogToRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "grantPrivilegeOnCatalogToRole",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnCatalogToRole(
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testGrantPrivilegeOnCatalogToRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "grantPrivilegeOnCatalogToRole",
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
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnCatalogFromRoleSufficientPrivileges() {
    return doTestSufficientPrivilegeSets(
        "revokePrivilegeOnCatalogFromRole",
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.CATALOG_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnCatalogFromRole(
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testRevokePrivilegeOnCatalogFromRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "revokePrivilegeOnCatalogFromRole",
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
                    CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnNamespaceToRoleSufficientPrivileges() {
    return Stream.of(CATALOG_NAME, FEDERATED_CATALOG_NAME)
        .flatMap(
            catalogName ->
                doTestSufficientPrivileges(
                    "grantPrivilegeOnNamespaceToRole[" + catalogName + "]",
                    List.of(
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS,
                        PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
                    () ->
                        newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                            .grantPrivilegeOnNamespaceToRole(
                                catalogName,
                                CATALOG_ROLE2,
                                NS1,
                                PolarisPrivilege.CATALOG_MANAGE_ACCESS),
                    null, // cleanupAction
                    (privilege) ->
                        adminService.grantPrivilegeOnCatalogToRole(
                            catalogName, CATALOG_ROLE1, privilege),
                    (privilege) ->
                        adminService.revokePrivilegeOnCatalogFromRole(
                            catalogName, CATALOG_ROLE1, privilege)));
  }

  @TestFactory
  Stream<DynamicNode>
      testGrantPrivilegeOnNamespaceToRoleSufficientPrivileges_FederationNestedNamespace() {
    return doTestSufficientPrivileges(
        "grantPrivilegeOnNamespaceToRole_FederationNestedNamespace",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnNamespaceToRole(
                    FEDERATED_CATALOG_NAME,
                    CATALOG_ROLE2,
                    NS1AA,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null, // cleanupAction
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(
                FEDERATED_CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(
                FEDERATED_CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @TestFactory
  Stream<DynamicTest> testGrantPrivilegeOnNamespaceToRoleInsufficientPrivileges() {
    return Stream.of(CATALOG_NAME, FEDERATED_CATALOG_NAME)
        .flatMap(
            catalogName ->
                doTestInsufficientPrivileges(
                    "grantPrivilegeOnNamespaceToRole[" + catalogName + "]",
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
                                catalogName,
                                CATALOG_ROLE2,
                                NS1,
                                PolarisPrivilege.CATALOG_MANAGE_ACCESS),
                    (privilege) ->
                        adminService.grantPrivilegeOnCatalogToRole(
                            catalogName, CATALOG_ROLE1, privilege),
                    (privilege) ->
                        adminService.revokePrivilegeOnCatalogFromRole(
                            catalogName, CATALOG_ROLE1, privilege)));
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnNamespaceFromRoleSufficientPrivileges() {
    return doTestSufficientPrivilegeSets(
        "revokePrivilegeOnNamespaceFromRole",
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_ACCESS),
            Set.of(
                PolarisPrivilege.NAMESPACE_MANAGE_GRANTS_ON_SECURABLE,
                PolarisPrivilege.CATALOG_ROLE_MANAGE_GRANTS_FOR_GRANTEE)),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .revokePrivilegeOnNamespaceFromRole(
                    CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testRevokePrivilegeOnNamespaceFromRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "revokePrivilegeOnNamespaceFromRole",
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
                    CATALOG_NAME, CATALOG_ROLE2, NS1, PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnTableToRoleSufficientPrivileges() {
    return Stream.of(CATALOG_NAME, FEDERATED_CATALOG_NAME)
        .flatMap(
            catalogName ->
                doTestSufficientPrivileges(
                    "grantPrivilegeOnTableToRole[" + catalogName + "]",
                    List.of(
                        PolarisPrivilege.CATALOG_MANAGE_ACCESS,
                        PolarisPrivilege.TABLE_MANAGE_GRANTS_ON_SECURABLE),
                    () ->
                        newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                            .grantPrivilegeOnTableToRole(
                                catalogName,
                                CATALOG_ROLE2,
                                TABLE_NS1_1,
                                PolarisPrivilege.CATALOG_MANAGE_ACCESS),
                    null, // cleanupAction
                    (privilege) ->
                        adminService.grantPrivilegeOnCatalogToRole(
                            catalogName, CATALOG_ROLE1, privilege),
                    (privilege) ->
                        adminService.revokePrivilegeOnCatalogFromRole(
                            catalogName, CATALOG_ROLE1, privilege)));
  }

  @TestFactory
  Stream<DynamicTest> testGrantPrivilegeOnTableToRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "grantPrivilegeOnTableToRole",
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
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnTableFromRoleSufficientPrivileges() {
    return doTestSufficientPrivilegeSets(
        "revokePrivilegeOnTableFromRole",
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
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testRevokePrivilegeOnTableFromRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "revokePrivilegeOnTableFromRole",
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
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnViewToRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "grantPrivilegeOnViewToRole",
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
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testGrantPrivilegeOnViewToRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "grantPrivilegeOnViewToRole",
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
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testRevokePrivilegeOnViewFromRoleSufficientPrivileges() {
    return doTestSufficientPrivilegeSets(
        "revokePrivilegeOnViewFromRole",
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
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testRevokePrivilegeOnViewFromRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "revokePrivilegeOnViewFromRole",
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
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }

  @TestFactory
  Stream<DynamicNode> testGrantPrivilegeOnPolicyToRoleSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "grantPrivilegeOnPolicyToRole",
        List.of(
            PolarisPrivilege.CATALOG_MANAGE_ACCESS,
            PolarisPrivilege.POLICY_MANAGE_GRANTS_ON_SECURABLE),
        () ->
            newTestAdminService(Set.of(PRINCIPAL_ROLE1))
                .grantPrivilegeOnPolicyToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    POLICY_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS),
        null // cleanupAction
        );
  }

  @TestFactory
  Stream<DynamicTest> testGrantPrivilegeOnPolicyToRoleInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "grantPrivilegeOnPolicyToRole",
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
                .grantPrivilegeOnPolicyToRole(
                    CATALOG_NAME,
                    CATALOG_ROLE2,
                    POLICY_NS1_1,
                    PolarisPrivilege.CATALOG_MANAGE_ACCESS));
  }
}
