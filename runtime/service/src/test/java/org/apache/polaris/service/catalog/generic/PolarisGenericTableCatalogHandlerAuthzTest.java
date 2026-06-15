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
package org.apache.polaris.service.catalog.generic;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrivilegeResult;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.admin.PolarisAuthzTestsFactory;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(Profiles.PolarisAuthzBaseProfile.class)
public class PolarisGenericTableCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  @Inject GenericTableCatalogHandlerFactory genericTableCatalogHandlerFactory;

  private GenericTableCatalogHandler newWrapper() {
    return newWrapper(Set.of());
  }

  private GenericTableCatalogHandler newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(activatedPrincipalRoles, CATALOG_NAME);
  }

  private GenericTableCatalogHandler newWrapper(
      Set<String> activatedPrincipalRoles, String catalogName) {
    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);
    return genericTableCatalogHandlerFactory.createHandler(catalogName, authenticatedPrincipal);
  }

  @TestFactory
  Stream<DynamicNode> testListGenericTablesPrivileges() {
    return authzTestsBuilder("listGenericTables")
        .action(() -> newWrapper().listGenericTables(NS1A))
        .shouldPassWith(PolarisPrivilege.TABLE_LIST)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateGenericTablePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA));

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("createGenericTable")
        .action(
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE1))
                    .createGenericTable(newtable, "format", "file:///temp/", "doc", Map.of()))
        .cleanupAction(() -> newWrapper(Set.of(PRINCIPAL_ROLE2)).dropGenericTable(newtable))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadGenericTablePrivileges() {
    return authzTestsBuilder("loadGenericTable")
        .action(() -> newWrapper().loadGenericTable(TABLE_NS1_1_GENERIC))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropGenericTablePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

    return authzTestsBuilder("dropGenericTable")
        .action(() -> newWrapper(Set.of(PRINCIPAL_ROLE1)).dropGenericTable(TABLE_NS1_1_GENERIC))
        .cleanupAction(
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE2))
                    .createGenericTable(
                        TABLE_NS1_1_GENERIC, "format", "file:///temp/", "doc", Map.of()))
        .shouldPassWith(PolarisPrivilege.TABLE_DROP)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }

  private PolarisAuthzTestsFactory.Builder tableLevelAuthzTestsBuilder(
      String operationName, TableIdentifier tableId) {
    return authzTestsBuilder(operationName)
        .grantAction(
            priv ->
                adminService.grantPrivilegeOnTableToRole(
                    CATALOG_NAME, CATALOG_ROLE1, tableId, priv))
        .revokeAction(
            priv -> {
              PrivilegeResult res =
                  adminService.revokePrivilegeOnTableFromRole(
                      CATALOG_NAME, CATALOG_ROLE1, tableId, priv);
              // After table drop + recreate, grants on the old entity no longer exist on the
              // new entity. Only treat GRANT_NOT_FOUND or ENTITY_NOT_FOUND as acceptable —
              // any other failure should propagate to avoid masking real errors.
              if (!res.isSuccess()) {
                BaseResult.ReturnStatus status = res.getReturnStatus();
                if (status == BaseResult.ReturnStatus.GRANT_NOT_FOUND
                    || status == BaseResult.ReturnStatus.ENTITY_NOT_FOUND) {
                  return new PrivilegeResult(new PolarisGrantRecord());
                }
              }
              return res;
            });
  }

  /**
   * Tests that dropGenericTable works with table-level privilege grants. This verifies that the
   * authorization resolves the table entity (not just the namespace), which is required for
   * table-level grants to take effect.
   */
  @TestFactory
  Stream<DynamicNode> testDropGenericTableWithTableLevelPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

    return tableLevelAuthzTestsBuilder("dropGenericTableWithTableLevelGrant", TABLE_NS1_1_GENERIC)
        .action(() -> newWrapper(Set.of(PRINCIPAL_ROLE1)).dropGenericTable(TABLE_NS1_1_GENERIC))
        .cleanupAction(
            () ->
                newWrapper(Set.of(PRINCIPAL_ROLE2))
                    .createGenericTable(
                        TABLE_NS1_1_GENERIC, "format", "file:///temp/", "doc", Map.of()))
        .shouldPassWith(PolarisPrivilege.TABLE_DROP)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .createTests();
  }
}
