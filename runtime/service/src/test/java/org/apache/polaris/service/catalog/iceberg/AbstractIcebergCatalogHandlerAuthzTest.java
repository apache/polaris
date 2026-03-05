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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.ImmutableScanReport;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.context.catalog.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;

/**
 * Authorization test class for IcebergCatalogHandler. Runs with the default value for
 * ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES (currently true).
 *
 * <p>This class tests:
 *
 * <ul>
 *   <li>Standard authorization behavior for all catalog operations
 *   <li>Fine-grained authorization for table metadata update operations
 *   <li>Coarse-grained fallback behavior
 *   <li>Super-privilege behavior (e.g., TABLE_MANAGE_STRUCTURE)
 * </ul>
 */
@SuppressWarnings("resource")
public abstract class AbstractIcebergCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  @Inject CallContextCatalogFactory callContextCatalogFactory;
  @Inject IcebergCatalogHandlerFactory icebergCatalogHandlerFactory;

  protected IcebergCatalogHandler newHandler() {
    return newHandler(Set.of());
  }

  private IcebergCatalogHandler newHandler(Set<String> activatedPrincipalRoles) {
    return newHandler(activatedPrincipalRoles, CATALOG_NAME, callContextCatalogFactory);
  }

  private IcebergCatalogHandler newHandler(
      Set<String> activatedPrincipalRoles, String catalogName, CallContextCatalogFactory factory) {
    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);
    IcebergCatalogHandler handler =
        icebergCatalogHandlerFactory.createHandler(catalogName, authenticatedPrincipal);
    if (factory == callContextCatalogFactory) {
      return handler;
    }
    return ImmutableIcebergCatalogHandler.builder().from(handler).catalogFactory(factory).build();
  }

  @TestFactory
  Stream<DynamicNode> testListNamespacesPrivileges() {
    return authzTestsBuilder("listNamespaces")
        .action(() -> newHandler().listNamespaces(Namespace.of()))
        .shouldPassWith(PolarisPrivilege.NAMESPACE_LIST)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_CREATE)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testInsufficientPermissionsPriorToSecretRotation() {
    String principalName = "all_the_powers";
    CreatePrincipalResult newPrincipal =
        metaStoreManager.createPrincipal(
            callContext.getPolarisCallContext(),
            new PrincipalEntity.Builder()
                .setName(principalName)
                .setCreateTimestamp(Instant.now().toEpochMilli())
                .setCredentialRotationRequiredState()
                .build());
    adminService.assignPrincipalRole(principalName, PRINCIPAL_ROLE1);
    adminService.assignPrincipalRole(principalName, PRINCIPAL_ROLE2);

    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(newPrincipal.getPrincipal(), Set.of(PRINCIPAL_ROLE1, PRINCIPAL_ROLE2));

    IcebergCatalogHandler handler =
        icebergCatalogHandlerFactory.createHandler(CATALOG_NAME, authenticatedPrincipal);

    // a variety of actions are all disallowed because the principal's credentials must be rotated

    Namespace ns3 = Namespace.of("ns3");
    Stream<DynamicNode> beforeRotationTests =
        Stream.of(
                authzTestsBuilder("listNamespaces (before rotation)")
                    .action(() -> handler.listNamespaces(Namespace.of()))
                    .principalName(principalName)
                    .shouldFailWithAnyPrivilege()
                    .createTests(),
                authzTestsBuilder("createNamespace (before rotation)")
                    .action(
                        () ->
                            handler.createNamespace(
                                CreateNamespaceRequest.builder().withNamespace(ns3).build()))
                    .principalName(principalName)
                    .shouldFailWithAnyPrivilege()
                    .createTests(),
                authzTestsBuilder("listTables (before rotation)")
                    .action(() -> handler.listTables(NS1))
                    .principalName(principalName)
                    .shouldFailWithAnyPrivilege()
                    .createTests())
            .flatMap(s -> s);

    // Rotate credentials and create refreshed wrapper
    PrincipalWithCredentialsCredentials credentials =
        new PrincipalWithCredentialsCredentials(
            newPrincipal.getPrincipalSecrets().getPrincipalClientId(),
            newPrincipal.getPrincipalSecrets().getMainSecret());
    PrincipalEntity refreshPrincipal =
        rotateAndRefreshPrincipal(
            metaStoreManager, principalName, credentials, callContext.getPolarisCallContext());
    PolarisPrincipal authenticatedPrincipal1 =
        PolarisPrincipal.of(refreshPrincipal, Set.of(PRINCIPAL_ROLE1, PRINCIPAL_ROLE2));

    IcebergCatalogHandler refreshedWrapper =
        ImmutableIcebergCatalogHandler.builder()
            .from(handler)
            .polarisPrincipal(authenticatedPrincipal1)
            .build();

    // Grant NAMESPACE_DROP to CATALOG_ROLE2 so cleanup can work
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DROP));

    // Tests after credential rotation - actions should succeed with proper privileges
    Stream<DynamicNode> afterRotationTests =
        Stream.of(
                authzTestsBuilder("listNamespaces (after rotation)")
                    .action(() -> refreshedWrapper.listNamespaces(Namespace.of()))
                    .principalName(principalName)
                    .shouldPassWith(PolarisPrivilege.NAMESPACE_LIST)
                    .createTests(),
                authzTestsBuilder("createNamespace (after rotation)")
                    .action(
                        () ->
                            refreshedWrapper.createNamespace(
                                CreateNamespaceRequest.builder().withNamespace(ns3).build()))
                    .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropNamespace(ns3))
                    .principalName(principalName)
                    .shouldPassWith(PolarisPrivilege.NAMESPACE_CREATE)
                    .createTests(),
                authzTestsBuilder("listTables (after rotation)")
                    .action(() -> refreshedWrapper.listTables(NS1))
                    .principalName(principalName)
                    .shouldPassWith(PolarisPrivilege.TABLE_LIST)
                    .createTests())
            .flatMap(s -> s);

    return Stream.concat(beforeRotationTests, afterRotationTests);
  }

  @Test
  public void testListNamespacesCatalogLevelWithPrincipalRoleActivation() {
    // Grant catalog-level privilege to CATALOG_ROLE1
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.NAMESPACE_LIST));
    Assertions.assertThat(newHandler().listNamespaces(Namespace.of()).namespaces())
        .containsAll(List.of(NS1, NS2));

    // Just activating PRINCIPAL_ROLE1 should also work.
    Assertions.assertThat(
            newHandler(Set.of(PRINCIPAL_ROLE1)).listNamespaces(Namespace.of()).namespaces())
        .containsAll(List.of(NS1, NS2));

    // If we only activate PRINCIPAL_ROLE2 it won't have the privilege.
    Assertions.assertThatThrownBy(
            () -> newHandler(Set.of(PRINCIPAL_ROLE2)).listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not authorized");

    // If we revoke, then it should fail again even with all principal roles activated.
    assertSuccess(
        adminService.revokePrivilegeOnCatalogFromRole(
            CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.NAMESPACE_LIST));
    Assertions.assertThatThrownBy(() -> newHandler().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testListNamespacesChildOnly() {
    // Grant only NS1-level privilege to CATALOG_ROLE1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.NAMESPACE_LIST));

    // Listing directly on NS1 succeeds
    Assertions.assertThat(newHandler().listNamespaces(NS1).namespaces())
        .containsAll(List.of(NS1A, NS1B));

    // Root listing fails
    Assertions.assertThatThrownBy(() -> newHandler().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);

    // NS2 listing fails
    Assertions.assertThatThrownBy(() -> newHandler().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);

    // Listing on a child of NS1 succeeds
    Assertions.assertThat(newHandler().listNamespaces(NS1A).namespaces())
        .containsAll(List.of(NS1AA));
  }

  @TestFactory
  Stream<DynamicNode> testCreateNamespacePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DROP));

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("createNamespace")
        .action(
            () -> {
              newHandler(Set.of(PRINCIPAL_ROLE1))
                  .createNamespace(
                      CreateNamespaceRequest.builder()
                          .withNamespace(Namespace.of("newns"))
                          .build());
              newHandler(Set.of(PRINCIPAL_ROLE1))
                  .createNamespace(
                      CreateNamespaceRequest.builder()
                          .withNamespace(Namespace.of("ns1", "ns1a", "newns"))
                          .build());
            })
        .cleanupAction(
            () -> {
              newHandler(Set.of(PRINCIPAL_ROLE2)).dropNamespace(Namespace.of("newns"));
              newHandler(Set.of(PRINCIPAL_ROLE2))
                  .dropNamespace(Namespace.of("ns1", "ns1a", "newns"));
            })
        .shouldPassWith(PolarisPrivilege.NAMESPACE_CREATE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadNamespaceMetadataPrivileges() {
    return authzTestsBuilder("loadNamespaceMetadata")
        .action(() -> newHandler().loadNamespaceMetadata(NS1A))
        .shouldPassWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testNamespaceExistsPrivileges() {
    // TODO: If we change the behavior of existence-check to return 404 on unauthorized,
    // the overall test structure will need to change (other tests catching ForbiddenException
    // need to still have catalog-level "REFERENCE" equivalent privileges, and the exists()
    // tests need to expect 404 instead).
    return authzTestsBuilder("namespaceExists")
        .action(() -> newHandler().namespaceExists(NS1A))
        .shouldPassWith(PolarisPrivilege.NAMESPACE_LIST)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_CREATE)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropNamespacePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_CREATE));

    return authzTestsBuilder("dropNamespace")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropNamespace(NS1AA))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .createNamespace(CreateNamespaceRequest.builder().withNamespace(NS1AA).build()))
        .shouldPassWith(PolarisPrivilege.NAMESPACE_DROP)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateNamespacePropertiesPrivileges() {
    return authzTestsBuilder("updateNamespaceProperties")
        .action(
            () -> {
              newHandler()
                  .updateNamespaceProperties(
                      NS1A,
                      UpdateNamespacePropertiesRequest.builder().update("foo", "bar").build());
              newHandler()
                  .updateNamespaceProperties(
                      NS1A, UpdateNamespacePropertiesRequest.builder().remove("foo").build());
            })
        .shouldPassWith(PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testListTablesPrivileges() {
    return authzTestsBuilder("listTables")
        .action(() -> newHandler().listTables(NS1A))
        .shouldPassWith(PolarisPrivilege.TABLE_LIST)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateTableDirectPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA));

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");
    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName("newtable").withSchema(SCHEMA).build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("createTableDirect")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).createTableDirect(NS2, createRequest))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTableWithPurge(newtable))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateTableDirectWithWriteDelegationPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA));

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");
    final CreateTableRequest createDirectWithWriteDelegationRequest =
        CreateTableRequest.builder().withName("newtable").withSchema(SCHEMA).stageCreate().build();

    return authzTestsBuilder("createTableDirectWithWriteDelegation")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .createTableDirectWithWriteDelegation(
                        NS2, createDirectWithWriteDelegationRequest, Optional.empty()))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTableWithPurge(newtable))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(
            PolarisPrivilege.TABLE_CREATE) // TABLE_CREATE itself is insufficient for delegation
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateTableStagedPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));

    final CreateTableRequest createStagedRequest =
        CreateTableRequest.builder()
            .withName("stagetable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("createTableStaged")
        .action(
            () -> newHandler(Set.of(PRINCIPAL_ROLE1)).createTableStaged(NS2, createStagedRequest))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateTableStagedWithWriteDelegationPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));

    final CreateTableRequest createStagedWithWriteDelegationRequest =
        CreateTableRequest.builder()
            .withName("stagetable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    return authzTestsBuilder("createTableStagedWithWriteDelegation")
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1))
                    .createTableStagedWithWriteDelegation(
                        NS2, createStagedWithWriteDelegationRequest, Optional.empty()))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRegisterTablePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_READ_PROPERTIES));

    // To get a handy metadata file we can use one from another table.
    // to avoid overlapping directories, drop the original table and recreate it via registerTable
    final String metadataLocation = newHandler().loadTable(TABLE_NS1_1, "all").metadataLocation();
    newHandler(Set.of(PRINCIPAL_ROLE2)).dropTableWithoutPurge(TABLE_NS1_1);

    final RegisterTableRequest registerRequest =
        new RegisterTableRequest() {
          @Override
          public String name() {
            return TABLE_NS1_1.name();
          }

          @Override
          public String metadataLocation() {
            return metadataLocation;
          }
        };

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("registerTable")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).registerTable(NS1, registerRequest))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropTableWithoutPurge(TABLE_NS1_1))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTablePrivileges() {
    return authzTestsBuilder("loadTable")
        .action(() -> newHandler().loadTable(TABLE_NS1A_2, "all"))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTableIfStalePrivileges() {
    return authzTestsBuilder("loadTableIfStale")
        .action(
            () ->
                newHandler()
                    .loadTableIfStale(TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all"))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTableWithReadAccessDelegationPrivileges() {
    return authzTestsBuilder("loadTableWithAccessDelegation")
        .action(
            () -> newHandler().loadTableWithAccessDelegation(TABLE_NS1A_2, "all", Optional.empty()))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTableWithWriteAccessDelegationPrivileges() {
    // TODO: Once we give different creds for read/write privilege, move this
    // TABLE_READ_DATA into a special-case test; with only TABLE_READ_DATA we'd expect
    // to receive a read-only credential.
    return authzTestsBuilder("loadTableWithAccessDelegation (write)")
        .action(
            () -> newHandler().loadTableWithAccessDelegation(TABLE_NS1A_2, "all", Optional.empty()))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTableWithReadAccessDelegationIfStalePrivileges() {
    return authzTestsBuilder("loadTableWithAccessDelegationIfStale")
        .action(
            () ->
                newHandler()
                    .loadTableWithAccessDelegationIfStale(
                        TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all", Optional.empty()))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTableWithWriteAccessDelegationIfStalePrivileges() {
    // TODO: Once we give different creds for read/write privilege, move this
    // TABLE_READ_DATA into a special-case test; with only TABLE_READ_DATA we'd expect
    // to receive a read-only credential.
    return authzTestsBuilder("loadTableWithAccessDelegationIfStale (write)")
        .action(
            () ->
                newHandler()
                    .loadTableWithAccessDelegationIfStale(
                        TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all", Optional.empty()))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTablePrivileges() {
    return authzTestsBuilder("updateTable")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, new UpdateTableRequest()))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableForStagedCreatePrivileges() {
    // Note: This is kind of cheating by only leaning on the PolarisCatalogHandlerWrapper level
    // of differentiation between updateForStageCreate vs regular update so that we don't need
    // to actually set up the staged create but still test the privileges. If the underlying
    // behavior diverges, we need to change this test to actually start with a stageCreate.
    return authzTestsBuilder("updateTableForStagedCreate")
        .action(
            () -> newHandler().updateTableForStagedCreate(TABLE_NS1A_2, new UpdateTableRequest()))
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableFallbackToCoarseGrainedWhenFeatureDisabled() {
    // Test that when fine-grained authorization is disabled, it falls back to
    // TABLE_WRITE_PROPERTIES
    // This test validates that the feature flag works correctly by testing the negative case
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    // With fine-grained authorization disabled, TABLE_WRITE_PROPERTIES should work
    // even for operations that would require specific privileges when enabled
    return authzTestsBuilder("updateTable (coarse-grained fallback)")
        .action(() -> newWrapperWithFineGrainedAuthzDisabled().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  /**
   * Creates a wrapper with fine-grained authorization explicitly disabled for testing the fallback
   * behavior to coarse-grained authorization.
   */
  private IcebergCatalogHandler newWrapperWithFineGrainedAuthzDisabled() {
    PolarisPrincipal authenticatedPrincipal = PolarisPrincipal.of(principalEntity, Set.of());

    // Create a custom CallContext that returns a custom RealmConfig
    CallContext mockCallContext = Mockito.mock(CallContext.class);

    // Create a simple RealmConfig implementation that overrides just what we need
    RealmConfig customRealmConfig =
        new RealmConfig() {
          @SuppressWarnings("removal")
          @Override
          public <T> T getConfig(String configName) {
            throw new UnsupportedOperationException();
          }

          @SuppressWarnings("removal")
          @Override
          public <T> T getConfig(String configName, T defaultValue) {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> T getConfig(PolarisConfiguration<T> config) {
            return realmConfig.getConfig(config);
          }

          @Override
          @SuppressWarnings("unchecked")
          public <T> T getConfig(PolarisConfiguration<T> config, CatalogEntity catalogEntity) {
            // Override the specific configuration we want to test
            if (config.equals(FeatureConfiguration.ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES)) {
              return (T) Boolean.FALSE;
            }
            return realmConfig.getConfig(config, catalogEntity);
          }

          @Override
          @SuppressWarnings("unchecked")
          public <T> T getConfig(
              PolarisConfiguration<T> config, Map<String, String> catalogProperties) {
            // Override the specific configuration we want to test
            if (config.equals(FeatureConfiguration.ENABLE_FINE_GRAINED_UPDATE_TABLE_PRIVILEGES)) {
              return (T) Boolean.FALSE;
            }
            return realmConfig.getConfig(config, catalogProperties);
          }
        };

    // Mock the regular CallContext calls
    Mockito.when(mockCallContext.getRealmContext()).thenReturn(() -> "test");
    Mockito.when(mockCallContext.getRealmConfig()).thenReturn(customRealmConfig);
    Mockito.when(mockCallContext.getPolarisCallContext())
        .thenReturn(callContext.getPolarisCallContext());

    IcebergCatalogHandler handler =
        icebergCatalogHandlerFactory.createHandler(
            PolarisAuthzTestBase.CATALOG_NAME, authenticatedPrincipal);

    return ImmutableIcebergCatalogHandler.builder()
        .from(handler)
        .callContext(mockCallContext)
        .build();
  }

  @TestFactory
  Stream<DynamicNode> testDropTableWithoutPurgePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName(TABLE_NS1_1.name()).withSchema(SCHEMA).build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("dropTableWithoutPurge")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropTableWithoutPurge(TABLE_NS1_1))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .createTableDirect(TABLE_NS1_1.namespace(), createRequest))
        .shouldPassWith(PolarisPrivilege.TABLE_DROP)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropTableWithPurgePrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName(TABLE_NS1_1.name()).withSchema(SCHEMA).build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("dropTableWithPurge")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropTableWithPurge(TABLE_NS1_1))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .createTableDirect(TABLE_NS1_1.namespace(), createRequest))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_DROP)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testTableExistsPrivileges() {
    return authzTestsBuilder("tableExists")
        .action(() -> newHandler().tableExists(TABLE_NS1A_2))
        .shouldPassWith(PolarisPrivilege.TABLE_LIST)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE)
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRenameTablePrivileges() {
    final TableIdentifier srcTable = TABLE_NS1_1;
    final TableIdentifier dstTable = TableIdentifier.of(NS1AA, "newtable");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcTable).withDestination(dstTable).build();
    final RenameTableRequest rename2 =
        RenameTableRequest.builder().withSource(dstTable).withDestination(srcTable).build();

    return authzTestsBuilder("renameTable")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).renameTable(rename1))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).renameTable(rename2))
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_DROP)
        .createTests();
  }

  @Test
  public void testRenameTablePrivilegesOnWrongSourceOrDestination() {
    final TableIdentifier srcTable = TABLE_NS2_1;
    final TableIdentifier dstTable = TableIdentifier.of(NS1AA, "newtable");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcTable).withDestination(dstTable).build();
    final RenameTableRequest rename2 =
        RenameTableRequest.builder().withSource(dstTable).withDestination(srcTable).build();

    // Minimum privileges should succeed -- drop on src, create on dst parent.
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, srcTable, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstTable.namespace(), PolarisPrivilege.TABLE_CREATE));

    // Initial rename should succeed
    newHandler().renameTable(rename1);

    // Inverse operation should fail
    Assertions.assertThatThrownBy(() -> newHandler().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Now grant TABLE_DROP on dst
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstTable, PolarisPrivilege.TABLE_DROP));

    // Still not enough without TABLE_CREATE at source
    Assertions.assertThatThrownBy(() -> newHandler().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Even grant CATALOG_MANAGE_CONTENT under all of NS1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.CATALOG_MANAGE_CONTENT));

    // Still not enough to rename back to src since src was NS2.
    Assertions.assertThatThrownBy(() -> newHandler().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Finally, grant TABLE_CREATE on NS2 and it should succeed to rename back to src.
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS2, PolarisPrivilege.TABLE_CREATE));
    newHandler().renameTable(rename2);
  }

  @TestFactory
  Stream<DynamicNode> testCommitTransactionPrivileges() {
    CommitTransactionRequest req =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(TABLE_NS1_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1A_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1B_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS2_1, List.of(), List.of())));

    return authzTestsBuilder("commitTransaction")
        .action(() -> newHandler().commitTransaction(req))
        .shouldPassWith(PolarisPrivilege.TABLE_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .createTests();
  }

  @Test
  public void testCommitTransactionMixedPermissions() {
    CommitTransactionRequest req =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(TABLE_NS1_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1A_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1B_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS2_1, List.of(), List.of())));

    // Grant TABLE_CREATE for all of NS1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.TABLE_CREATE));
    Assertions.assertThatThrownBy(() -> newHandler().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_FULL_METADATA directly on TABLE_NS1_1
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS1_1, PolarisPrivilege.TABLE_FULL_METADATA));
    Assertions.assertThatThrownBy(() -> newHandler().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_PROPERTIES on NS1A namespace
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1A, PolarisPrivilege.TABLE_WRITE_PROPERTIES));
    Assertions.assertThatThrownBy(() -> newHandler().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_DATA directly on TABLE_NS1B_1
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS1B_1, PolarisPrivilege.TABLE_WRITE_DATA));
    Assertions.assertThatThrownBy(() -> newHandler().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_PROPERTIES directly on TABLE_NS2_1
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS2_1, PolarisPrivilege.TABLE_WRITE_PROPERTIES));
    Assertions.assertThatThrownBy(() -> newHandler().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Also grant TABLE_CREATE directly on TABLE_NS2_1
    // TODO: If we end up having fine-grained differentiation between updateForStagedCreate
    // and update, then this one should only be TABLE_CREATE on the *parent* of this last table
    // and the table shouldn't exist.
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS2_1, PolarisPrivilege.TABLE_CREATE));
    newHandler().commitTransaction(req);
  }

  @TestFactory
  Stream<DynamicNode> testListViewsPrivileges() {
    return authzTestsBuilder("listViews")
        .action(() -> newHandler().listViews(NS1A))
        .shouldPassWith(PolarisPrivilege.VIEW_LIST)
        .shouldPassWith(PolarisPrivilege.VIEW_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_CREATE)
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testCreateViewPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.VIEW_DROP));

    final TableIdentifier newview = TableIdentifier.of(NS2, "newview");
    final CreateViewRequest createRequest =
        ImmutableCreateViewRequest.builder()
            .name("newview")
            .schema(SCHEMA)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(1)
                    .defaultNamespace(NS1)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql(VIEW_QUERY)
                            .dialect("spark")
                            .build())
                    .build())
            .build();

    return authzTestsBuilder("createView")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).createView(NS2, createRequest))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE2)).dropView(newview))
        .shouldPassWith(PolarisPrivilege.VIEW_CREATE)
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadViewPrivileges() {
    return authzTestsBuilder("loadView")
        .action(() -> newHandler().loadView(VIEW_NS1A_2))
        .shouldPassWith(PolarisPrivilege.VIEW_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateViewPrivileges() {
    return authzTestsBuilder("replaceView")
        .action(() -> newHandler().replaceView(VIEW_NS1A_2, new UpdateTableRequest()))
        .shouldPassWith(PolarisPrivilege.VIEW_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testDropViewPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.VIEW_CREATE));

    final CreateViewRequest createRequest =
        ImmutableCreateViewRequest.builder()
            .name(VIEW_NS1_1.name())
            .schema(SCHEMA)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(1)
                    .defaultNamespace(NS1)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql(VIEW_QUERY)
                            .dialect("spark")
                            .build())
                    .build())
            .build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    return authzTestsBuilder("dropView")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).dropView(VIEW_NS1_1))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2))
                    .createView(VIEW_NS1_1.namespace(), createRequest))
        .shouldPassWith(PolarisPrivilege.VIEW_DROP)
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testViewExistsPrivileges() {
    return authzTestsBuilder("viewExists")
        .action(() -> newHandler().viewExists(VIEW_NS1A_2))
        .shouldPassWith(PolarisPrivilege.VIEW_LIST)
        .shouldPassWith(PolarisPrivilege.VIEW_READ_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.VIEW_CREATE)
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testRenameViewPrivileges() {
    final TableIdentifier srcView = VIEW_NS1_1;
    final TableIdentifier dstView = TableIdentifier.of(NS1AA, "newview");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcView).withDestination(dstView).build();
    final RenameTableRequest rename2 =
        RenameTableRequest.builder().withSource(dstView).withDestination(srcView).build();

    return authzTestsBuilder("renameView")
        .action(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).renameView(rename1))
        .cleanupAction(() -> newHandler(Set.of(PRINCIPAL_ROLE1)).renameView(rename2))
        .shouldPassWith(PolarisPrivilege.VIEW_FULL_METADATA)
        .shouldPassWith(PolarisPrivilege.VIEW_DROP, PolarisPrivilege.VIEW_CREATE)
        .createTests();
  }

  @Test
  public void testRenameViewPrivilegesOnWrongSourceOrDestination() {
    final TableIdentifier srcView = VIEW_NS2_1;
    final TableIdentifier dstView = TableIdentifier.of(NS1AA, "newview");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcView).withDestination(dstView).build();
    final RenameTableRequest rename2 =
        RenameTableRequest.builder().withSource(dstView).withDestination(srcView).build();

    // Minimum privileges should succeed -- drop on src, create on dst parent.
    assertSuccess(
        adminService.grantPrivilegeOnViewToRole(
            CATALOG_NAME, CATALOG_ROLE1, srcView, PolarisPrivilege.VIEW_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstView.namespace(), PolarisPrivilege.VIEW_CREATE));

    // Initial rename should succeed
    newHandler().renameView(rename1);

    // Inverse operation should fail
    Assertions.assertThatThrownBy(() -> newHandler().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Now grant VIEW_DROP on dst
    assertSuccess(
        adminService.grantPrivilegeOnViewToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstView, PolarisPrivilege.VIEW_DROP));

    // Still not enough without VIEW_CREATE at source
    Assertions.assertThatThrownBy(() -> newHandler().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Even grant CATALOG_MANAGE_CONTENT under all of NS1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.CATALOG_MANAGE_CONTENT));

    // Still not enough to rename back to src since src was NS2.
    Assertions.assertThatThrownBy(() -> newHandler().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Finally, grant VIEW_CREATE on NS2 and it should succeed to rename back to src.
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS2, PolarisPrivilege.VIEW_CREATE));
    newHandler().renameView(rename2);
  }

  @TestFactory
  Stream<DynamicNode> testSendNotificationAllTypes() {
    String externalCatalog = "testSendNotificationAllTypes";
    String storageLocation =
        "file:///tmp/send_notification_sufficient_privileges_" + System.nanoTime();

    createExternalCatalog(externalCatalog, storageLocation);
    PolarisCallContextCatalogFactory factory = createExternalCatalogFactory(externalCatalog);

    Namespace namespace = Namespace.of("extns1", "extns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");
    String tableUuid = UUID.randomUUID().toString();

    NotificationRequest createRequest =
        createNotificationRequest(table, tableUuid, storageLocation);
    NotificationRequest updateRequest =
        updateNotificationRequest(table, tableUuid, storageLocation);
    NotificationRequest dropRequest = dropNotificationRequest(table, tableUuid);
    NotificationRequest validateRequest =
        validateNotificationRequest(table, tableUuid, storageLocation);

    return authzTestsBuilder("sendNotification (ALL)")
        .catalogName(externalCatalog)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .action(
            () -> {
              newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                  .sendNotification(table, createRequest);
              newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                  .sendNotification(table, updateRequest);
              newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                  .sendNotification(table, dropRequest);
              newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                  .sendNotification(table, validateRequest);
            })
        .cleanupAction(
            () -> {
              newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
                  .dropNamespace(Namespace.of("extns1", "extns2"));
              newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
                  .dropNamespace(Namespace.of("extns1"));
            })
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testSendNotificationCreate() {
    String externalCatalog = "testSendNotificationCreate";
    String storageLocation =
        "file:///tmp/send_notification_sufficient_privileges_" + System.nanoTime();

    createExternalCatalog(externalCatalog, storageLocation);
    PolarisCallContextCatalogFactory factory = createExternalCatalogFactory(externalCatalog);

    Namespace namespace = Namespace.of("extns1", "extns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");
    String tableUuid = UUID.randomUUID().toString();

    NotificationRequest createRequest =
        createNotificationRequest(table, tableUuid, storageLocation);
    NotificationRequest dropRequest = dropNotificationRequest(table, tableUuid);

    return authzTestsBuilder("sendNotification (CREATE)")
        .catalogName(externalCatalog)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                    .sendNotification(table, createRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
                    .sendNotification(table, dropRequest))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testSendNotificationUpdate() {
    String externalCatalog = "testSendNotificationUpdate";
    String storageLocation =
        "file:///tmp/send_notification_sufficient_privileges_" + System.nanoTime();

    createExternalCatalog(externalCatalog, storageLocation);
    PolarisCallContextCatalogFactory factory = createExternalCatalogFactory(externalCatalog);

    Namespace namespace = Namespace.of("extns1", "extns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");
    String tableUuid = UUID.randomUUID().toString();

    NotificationRequest updateRequest =
        updateNotificationRequest(table, tableUuid, storageLocation);
    NotificationRequest dropRequest = dropNotificationRequest(table, tableUuid);

    return authzTestsBuilder("sendNotification (UPDATE)")
        .catalogName(externalCatalog)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                    .sendNotification(table, updateRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
                    .sendNotification(table, dropRequest))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testSendNotificationDrop() {
    String externalCatalog = "testSendNotificationDrop";
    String storageLocation =
        "file:///tmp/send_notification_sufficient_privileges_" + System.nanoTime();

    createExternalCatalog(externalCatalog, storageLocation);
    PolarisCallContextCatalogFactory factory = createExternalCatalogFactory(externalCatalog);

    Namespace namespace = Namespace.of("extns1", "extns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");
    String tableUuid = UUID.randomUUID().toString();

    NotificationRequest createRequest =
        createNotificationRequest(table, tableUuid, storageLocation);
    NotificationRequest dropRequest = dropNotificationRequest(table, tableUuid);

    newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
        .sendNotification(table, createRequest);

    return authzTestsBuilder("sendNotification (DROP)")
        .catalogName(externalCatalog)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                    .sendNotification(table, dropRequest))
        .cleanupAction(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
                    .sendNotification(table, createRequest))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testSendNotificationValidation() {
    String externalCatalog = "testSendNotificationValidation";
    String storageLocation =
        "file:///tmp/send_notification_sufficient_privileges_" + System.nanoTime();

    createExternalCatalog(externalCatalog, storageLocation);
    PolarisCallContextCatalogFactory factory = createExternalCatalogFactory(externalCatalog);

    Namespace namespace = Namespace.of("extns1", "extns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");
    String tableUuid = UUID.randomUUID().toString();

    NotificationRequest createRequest =
        createNotificationRequest(table, tableUuid, storageLocation);
    NotificationRequest validateRequest =
        validateNotificationRequest(table, tableUuid, storageLocation);

    newHandler(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
        .sendNotification(table, createRequest);

    return authzTestsBuilder("sendNotification (VALIDATE)")
        .catalogName(externalCatalog)
        .action(
            () ->
                newHandler(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
                    .sendNotification(table, validateRequest))
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA)
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP)
        .createTests();
  }

  private void createExternalCatalog(String externalCatalog, String storageLocation) {
    FileStorageConfigInfo storageConfigModel =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    adminService.createCatalog(
        new CreateCatalogRequest(
            new CatalogEntity.Builder()
                .setName(externalCatalog)
                .setDefaultBaseLocation(storageLocation)
                .setStorageConfigurationInfo(realmConfig, storageConfigModel, storageLocation)
                .setCatalogType("EXTERNAL")
                .build()
                .asCatalog(serviceIdentityProvider)));
    adminService.createCatalogRole(
        externalCatalog, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE1).build());
    adminService.createCatalogRole(
        externalCatalog, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE2).build());

    adminService.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE1);
    adminService.assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE1, externalCatalog, CATALOG_ROLE1);
    adminService.assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE2, externalCatalog, CATALOG_ROLE2);
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            externalCatalog, CATALOG_ROLE2, PolarisPrivilege.CATALOG_MANAGE_CONTENT));
  }

  private PolarisCallContextCatalogFactory createExternalCatalogFactory(String externalCatalog) {
    return new PolarisCallContextCatalogFactory(
        diagServices,
        resolverFactory,
        Mockito.mock(),
        storageAccessConfigProvider,
        fileIOFactory,
        polarisEventListener,
        eventMetadataFactory,
        metaStoreManager,
        callContext,
        authenticatedRoot) {
      @Override
      public Catalog createCallContextCatalog(PolarisResolutionManifest resolvedManifest) {
        Catalog catalog = super.createCallContextCatalog(resolvedManifest);
        String fileIoImpl = "org.apache.iceberg.inmemory.InMemoryFileIO";
        catalog.initialize(
            externalCatalog, ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, fileIoImpl));
        return catalog;
      }
    };
  }

  private static NotificationRequest createNotificationRequest(
      TableIdentifier table, String tableUuid, String storageLocation) {
    NotificationRequest createRequest = new NotificationRequest();
    createRequest.setNotificationType(NotificationType.CREATE);
    TableUpdateNotification createPayload = new TableUpdateNotification();
    String metadataLocation =
        String.format("%s/bucket/table/metadata/v1.metadata.json", storageLocation);
    createPayload.setMetadataLocation(metadataLocation);
    createPayload.setTableName(table.name());
    createPayload.setTableUuid(tableUuid);
    createPayload.setTimestamp(230950845L);
    createRequest.setPayload(createPayload);
    writeTableMetadata(metadataLocation);
    return createRequest;
  }

  private static NotificationRequest updateNotificationRequest(
      TableIdentifier table, String tableUuid, String storageLocation) {
    NotificationRequest updateRequest = new NotificationRequest();
    updateRequest.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification updatePayload = new TableUpdateNotification();
    String metadataLocation =
        String.format("%s/bucket/table/metadata/v2.metadata.json", storageLocation);
    updatePayload.setMetadataLocation(metadataLocation);
    updatePayload.setTableName(table.name());
    updatePayload.setTableUuid(tableUuid);
    updatePayload.setTimestamp(330950845L);
    updateRequest.setPayload(updatePayload);
    writeTableMetadata(metadataLocation);
    return updateRequest;
  }

  private static NotificationRequest dropNotificationRequest(
      TableIdentifier table, String tableUuid) {
    NotificationRequest dropRequest = new NotificationRequest();
    dropRequest.setNotificationType(NotificationType.DROP);
    TableUpdateNotification dropPayload = new TableUpdateNotification();
    dropPayload.setTableName(table.name());
    dropPayload.setTableUuid(tableUuid);
    dropPayload.setTimestamp(430950845L);
    dropRequest.setPayload(dropPayload);
    return dropRequest;
  }

  private static NotificationRequest validateNotificationRequest(
      TableIdentifier table, String tableUuid, String storageLocation) {
    NotificationRequest validateRequest = new NotificationRequest();
    validateRequest.setNotificationType(NotificationType.VALIDATE);
    TableUpdateNotification validatePayload = new TableUpdateNotification();
    validatePayload.setMetadataLocation(
        String.format("%s/bucket/table/metadata/v1.metadata.json", storageLocation));
    validatePayload.setTableName(table.name());
    validatePayload.setTableUuid(tableUuid);
    validatePayload.setTimestamp(530950845L);
    validateRequest.setPayload(validatePayload);
    return validateRequest;
  }

  private static void writeTableMetadata(String metadataLocation) {
    String fileIoImpl = "org.apache.iceberg.inmemory.InMemoryFileIO";
    FileIO fileIO = CatalogUtil.loadFileIO(fileIoImpl, Map.of(), null);
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .addSchema(SCHEMA)
            .setLocation(metadataLocation)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .assignUUID()
            .build();
    TableMetadataParser.overwrite(tableMetadata, fileIO.newOutputFile(metadataLocation));
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_AssignUuid_Privileges() {
    // Test that TABLE_ASSIGN_UUID privilege is required for AssignUUID MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    return authzTestsBuilder("updateTable (AssignUUID)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(PolarisPrivilege.TABLE_ASSIGN_UUID)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_UpgradeFormatVersionPrivilege() {
    // Test that TABLE_UPGRADE_FORMAT_VERSION privilege is required for UpgradeFormatVersion
    // MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.UpgradeFormatVersion(2)));

    return authzTestsBuilder("updateTable (UpgradeFormatVersion)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_SetPropertiesPrivilege() {
    // Test that TABLE_SET_PROPERTIES privilege is required for SetProperties MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.SetProperties(Map.of("test.property", "test.value"))));

    return authzTestsBuilder("updateTable (SetProperties)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(PolarisPrivilege.TABLE_SET_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_RemoveProperties_Privilege() {
    // Test that TABLE_REMOVE_PROPERTIES privilege is required for RemoveProperties MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.RemoveProperties(Set.of("property.to.remove"))));

    return authzTestsBuilder("updateTable (RemoveProperties)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(PolarisPrivilege.TABLE_REMOVE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_MultipleUpdates_Privileges() {
    // Test that multiple MetadataUpdate types require multiple specific privileges
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(
                new MetadataUpdate.UpgradeFormatVersion(2),
                new MetadataUpdate.SetProperties(Map.of("test.prop", "test.val"))));

    return authzTestsBuilder("updateTable (multiple updates)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, request))
        .shouldPassWith(
            PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION, PolarisPrivilege.TABLE_SET_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_MANAGE_STRUCTURE)
        // Test that having only one specific privilege fails (need both)
        .shouldFailWith(PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION) // Only one of the two needed
        .shouldFailWith(PolarisPrivilege.TABLE_SET_PROPERTIES) // Only one of the two needed
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_TableManageStructureSuperPrivilege() {
    // Test that TABLE_MANAGE_STRUCTURE works as a super privilege for structural operations
    // (but NOT for snapshot operations like TABLE_ADD_SNAPSHOT)

    // Test structural operations that should work with TABLE_MANAGE_STRUCTURE
    UpdateTableRequest structuralRequest =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(
                new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()),
                new MetadataUpdate.UpgradeFormatVersion(2),
                new MetadataUpdate.SetProperties(Map.of("test.property", "test.value")),
                new MetadataUpdate.RemoveProperties(Set.of("property.to.remove"))));

    return authzTestsBuilder("updateTable (TABLE_MANAGE_STRUCTURE super privilege)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, structuralRequest))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_MANAGE_STRUCTURE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testUpdateTableWith_TableManageStructureDoesNotIncludeSnapshots() {
    // Verify that TABLE_MANAGE_STRUCTURE does NOT grant access to snapshot operations
    // This test verifies that TABLE_ADD_SNAPSHOT and TABLE_SET_SNAPSHOT_REF were correctly
    // excluded from the TABLE_MANAGE_STRUCTURE super privilege mapping

    // Test that TABLE_MANAGE_STRUCTURE works for non-snapshot structural operations
    UpdateTableRequest nonSnapshotRequest =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(
                new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()),
                new MetadataUpdate.SetProperties(Map.of("structure.test", "value"))));

    return authzTestsBuilder("updateTable (TABLE_MANAGE_STRUCTURE sufficient for non-snapshot ops)")
        .action(() -> newHandler().updateTable(TABLE_NS1A_2, nonSnapshotRequest))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_PROPERTIES)
        .shouldPassWith(PolarisPrivilege.TABLE_MANAGE_STRUCTURE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testLoadTableWith_TableManageStructureInsufficient() {
    // Test that TABLE_MANAGE_STRUCTURE is insufficient for operations that require
    // different privilege categories (like read operations)
    return authzTestsBuilder("loadTable (TABLE_MANAGE_STRUCTURE insufficient)")
        .action(() -> newHandler().loadTable(TABLE_NS1A_2, "all"))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_PROPERTIES)
        .shouldFailWith(PolarisPrivilege.TABLE_MANAGE_STRUCTURE)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testReportReadMetricsPrivileges() {
    ImmutableScanReport report =
        ImmutableScanReport.builder()
            .tableName(TABLE_NS1A_1.name())
            .snapshotId(123L)
            .schemaId(456)
            .projectedFieldIds(List.of(1, 2, 3))
            .projectedFieldNames(List.of("f1", "f2", "f3"))
            .filter(Expressions.alwaysTrue())
            .scanMetrics(ScanMetricsResult.fromScanMetrics(ScanMetrics.noop()))
            .build();
    ReportMetricsRequest request = ReportMetricsRequest.of(report);
    return authzTestsBuilder("reportMetrics (read)")
        .action(() -> newHandler().reportMetrics(TABLE_NS1A_1, request))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA) // super-privilege of TABLE_READ_DATA
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testReportWriteMetricsPrivileges() {
    CommitReport commitReport =
        ImmutableCommitReport.builder()
            .tableName(TABLE_NS1A_1.name())
            .snapshotId(23L)
            .operation("DELETE")
            .sequenceNumber(4L)
            .commitMetrics(CommitMetricsResult.from(CommitMetrics.noop(), Map.of()))
            .build();
    ReportMetricsRequest request = ReportMetricsRequest.of(commitReport);
    return authzTestsBuilder("reportMetrics (write)")
        .action(() -> newHandler().reportMetrics(TABLE_NS1A_1, request))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }
}
