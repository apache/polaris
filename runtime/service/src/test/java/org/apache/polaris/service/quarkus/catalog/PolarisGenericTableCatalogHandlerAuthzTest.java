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
package org.apache.polaris.service.quarkus.catalog;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.catalog.generic.GenericTableCatalogHandler;
import org.apache.polaris.service.quarkus.admin.PolarisAuthzTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class PolarisGenericTableCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  private GenericTableCatalogHandler newWrapper() {
    return newWrapper(Set.of());
  }

  private GenericTableCatalogHandler newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(activatedPrincipalRoles, CATALOG_NAME);
  }

  private GenericTableCatalogHandler newWrapper(
      Set<String> activatedPrincipalRoles, String catalogName) {
    final AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipal(principalEntity, activatedPrincipalRoles);
    return new GenericTableCatalogHandler(
        callContext,
        entityManager,
        metaStoreManager,
        securityContext(authenticatedPrincipal, activatedPrincipalRoles),
        catalogName,
        polarisAuthorizer);
  }

  /**
   * Tests each "sufficient" privilege individually using CATALOG_ROLE1 by granting at the
   * CATALOG_NAME level, revoking after each test, and also ensuring that the request fails after
   * revocation.
   *
   * @param sufficientPrivileges List of privileges that should be sufficient each in isolation for
   *     {@code action} to succeed.
   * @param action The operation being tested; could also be multiple operations that should all
   *     succeed with the sufficient privilege
   * @param cleanupAction If non-null, additional action to run to "undo" a previous success action
   *     in case the action has side effects. Called before revoking the sufficient privilege;
   *     either the cleanup privileges must be latent, or the cleanup action could be run with
   *     PRINCIPAL_ROLE2 while runnint {@code action} with PRINCIPAL_ROLE1.
   */
  private void doTestSufficientPrivileges(
      List<PolarisPrivilege> sufficientPrivileges, Runnable action, Runnable cleanupAction) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges.stream().map(priv -> Set.of(priv)).toList(),
        action,
        cleanupAction,
        PRINCIPAL_NAME);
  }

  /**
   * @param sufficientPrivileges each set of concurrent privileges expected to be sufficient
   *     together.
   * @param action
   * @param cleanupAction
   * @param principalName
   */
  private void doTestSufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> sufficientPrivileges,
      Runnable action,
      Runnable cleanupAction,
      String principalName) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges, action, cleanupAction, principalName, CATALOG_NAME);
  }

  /**
   * @param sufficientPrivileges each set of concurrent privileges expected to be sufficient
   *     together.
   * @param action
   * @param cleanupAction
   * @param principalName
   * @param catalogName
   */
  private void doTestSufficientPrivilegeSets(
      List<Set<PolarisPrivilege>> sufficientPrivileges,
      Runnable action,
      Runnable cleanupAction,
      String principalName,
      String catalogName) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges,
        action,
        cleanupAction,
        principalName,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(catalogName, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(catalogName, CATALOG_ROLE1, privilege));
  }

  private void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges, Runnable action) {
    doTestInsufficientPrivileges(insufficientPrivileges, PRINCIPAL_NAME, action);
  }

  /**
   * Tests each "insufficient" privilege individually using CATALOG_ROLE1 by granting at the
   * CATALOG_NAME level, ensuring the action fails, then revoking after each test case.
   */
  private void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges, String principalName, Runnable action) {
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        principalName,
        action,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testListGenericTablesAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().listGenericTables(NS1A),
        null /* cleanupAction */);
  }

  @Test
  public void testListGenericTablesInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().listGenericTables(NS1A));
  }

  @Test
  public void testCreateGenericTableAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA))
        .isTrue();

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createGenericTable(newtable, "format", "file:///temp/", "doc", Map.of());
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropGenericTable(newtable);
        });
  }

  @Test
  public void testCreateGenericTableInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createGenericTable(
                  TableIdentifier.of(NS2, "newtable"), "format", null, "doc", Map.of());
        });
  }

  @Test
  public void testLoadGenericTableSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadGenericTable(TABLE_NS1_1_GENERIC),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadTableInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().loadGenericTable(TABLE_NS1_1_GENERIC));
  }

  @Test
  public void testDropGenericTableAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE))
        .isTrue();

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropGenericTable(TABLE_NS1_1_GENERIC);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2))
              .createGenericTable(TABLE_NS1_1_GENERIC, "format", "file:///temp/", "doc", Map.of());
        });
  }

  @Test
  public void testDropGenericTableInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropGenericTable(TABLE_NS1_1_GENERIC);
        });
  }
}
