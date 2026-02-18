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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
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
  Stream<DynamicNode> testListGenericTablesSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "listGenericTables",
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

  @TestFactory
  Stream<DynamicTest> testListGenericTablesInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "listGenericTables",
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().listGenericTables(NS1A));
  }

  @TestFactory
  Stream<DynamicNode> testCreateGenericTableSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA));

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return doTestSufficientPrivileges(
        "createGenericTable",
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

  @TestFactory
  Stream<DynamicTest> testCreateGenericTableInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "createGenericTable",
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

  @TestFactory
  Stream<DynamicNode> testLoadGenericTableSufficientPrivileges() {
    return doTestSufficientPrivileges(
        "loadGenericTable",
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

  @TestFactory
  Stream<DynamicTest> testLoadGenericTableInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "loadGenericTable",
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().loadGenericTable(TABLE_NS1_1_GENERIC));
  }

  @TestFactory
  Stream<DynamicNode> testDropGenericTableSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

    return doTestSufficientPrivileges(
        "dropGenericTable",
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

  @TestFactory
  Stream<DynamicTest> testDropGenericTableInsufficientPrivileges() {
    return doTestInsufficientPrivileges(
        "dropGenericTable",
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
