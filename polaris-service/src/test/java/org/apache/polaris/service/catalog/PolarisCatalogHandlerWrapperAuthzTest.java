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
package org.apache.polaris.service.catalog;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.auth.AuthenticatedPolarisPrincipalImpl;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.context.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PolarisCatalogHandlerWrapperAuthzTest extends PolarisAuthzTestBase {
  private PolarisCatalogHandlerWrapper newWrapper() {
    return newWrapper(Set.of());
  }

  private PolarisCatalogHandlerWrapper newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(
        activatedPrincipalRoles, CATALOG_NAME, new TestPolarisCallContextCatalogFactory());
  }

  private PolarisCatalogHandlerWrapper newWrapper(
      Set<String> activatedPrincipalRoles,
      String catalogName,
      PolarisCallContextCatalogFactory factory) {
    final AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipalImpl(
            principalEntity.getId(), principalEntity.getName(), activatedPrincipalRoles);
    return new PolarisCatalogHandlerWrapper(
        callContext,
        entityManager,
        metaStoreManager,
        authenticatedPrincipal,
        factory,
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
  public void testListNamespacesAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_LIST,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().listNamespaces(Namespace.of()),
        null /* cleanupAction */);
  }

  @Test
  public void testListNamespacesInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_DROP),
        () -> newWrapper().listNamespaces(Namespace.of()));
  }

  @Test
  public void testInsufficientPermissionsPriorToSecretRotation() {
    String principalName = "all_the_powers";
    PolarisMetaStoreManager.CreatePrincipalResult newPrincipal =
        metaStoreManager.createPrincipal(
            callContext.getPolarisCallContext(),
            new PrincipalEntity.Builder()
                .setId(123)
                .setName(principalName)
                .setCreateTimestamp(Instant.now().toEpochMilli())
                .setCredentialRotationRequiredState()
                .build());
    adminService.assignPrincipalRole(principalName, PRINCIPAL_ROLE1);
    adminService.assignPrincipalRole(principalName, PRINCIPAL_ROLE2);

    final AuthenticatedPolarisPrincipal authenticatedPrincipal =
        new AuthenticatedPolarisPrincipalImpl(
            newPrincipal.getPrincipal().getId(), newPrincipal.getPrincipal().getName(), Set.of());
    PolarisCatalogHandlerWrapper wrapper =
        new PolarisCatalogHandlerWrapper(
            callContext,
            entityManager,
            metaStoreManager,
            authenticatedPrincipal,
            new TestPolarisCallContextCatalogFactory(),
            CATALOG_NAME,
            polarisAuthorizer);

    // a variety of actions are all disallowed because the principal's credentials must be rotated
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.values()),
        principalName,
        () -> wrapper.listNamespaces(Namespace.of()));
    Namespace ns3 = Namespace.of("ns3");
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.values()),
        principalName,
        () -> wrapper.createNamespace(CreateNamespaceRequest.builder().withNamespace(ns3).build()));
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.values()), principalName, () -> wrapper.listTables(NS1));
    PrincipalWithCredentialsCredentials credentials =
        new PrincipalWithCredentialsCredentials(
            newPrincipal.getPrincipalSecrets().getPrincipalClientId(),
            newPrincipal.getPrincipalSecrets().getMainSecret());
    PrincipalEntity refreshPrincipal =
        rotateAndRefreshPrincipal(
            metaStoreManager, principalName, credentials, callContext.getPolarisCallContext());
    final AuthenticatedPolarisPrincipal authenticatedPrincipal1 =
        new AuthenticatedPolarisPrincipalImpl(
            refreshPrincipal.getId(), refreshPrincipal.getName(), Set.of());
    PolarisCatalogHandlerWrapper refreshedWrapper =
        new PolarisCatalogHandlerWrapper(
            callContext,
            entityManager,
            metaStoreManager,
            authenticatedPrincipal1,
            new TestPolarisCallContextCatalogFactory(),
            CATALOG_NAME,
            polarisAuthorizer);

    doTestSufficientPrivilegeSets(
        List.of(Set.of(PolarisPrivilege.NAMESPACE_LIST)),
        () -> refreshedWrapper.listNamespaces(Namespace.of()),
        null,
        principalName);
    doTestSufficientPrivilegeSets(
        List.of(Set.of(PolarisPrivilege.NAMESPACE_CREATE)),
        () ->
            refreshedWrapper.createNamespace(
                CreateNamespaceRequest.builder().withNamespace(ns3).build()),
        null,
        principalName);
    doTestSufficientPrivilegeSets(
        List.of(Set.of(PolarisPrivilege.TABLE_LIST)),
        () -> refreshedWrapper.listTables(ns3),
        null,
        principalName);
  }

  @Test
  public void testListNamespacesCatalogLevelWithPrincipalRoleActivation() {
    // Grant catalog-level privilege to CATALOG_ROLE1
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.NAMESPACE_LIST))
        .isTrue();
    Assertions.assertThat(newWrapper().listNamespaces(Namespace.of()).namespaces())
        .containsAll(List.of(NS1, NS2));

    // Just activating PRINCIPAL_ROLE1 should also work.
    Assertions.assertThat(
            newWrapper(Set.of(PRINCIPAL_ROLE1)).listNamespaces(Namespace.of()).namespaces())
        .containsAll(List.of(NS1, NS2));

    // If we only activate PRINCIPAL_ROLE2 it won't have the privilege.
    Assertions.assertThatThrownBy(
            () -> newWrapper(Set.of(PRINCIPAL_ROLE2)).listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not authorized");

    // If we revoke, then it should fail again even with all principal roles activated.
    Assertions.assertThat(
            adminService.revokePrivilegeOnCatalogFromRole(
                CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.NAMESPACE_LIST))
        .isTrue();
    Assertions.assertThatThrownBy(() -> newWrapper().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testListNamespacesChildOnly() {
    // Grant only NS1-level privilege to CATALOG_ROLE1
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.NAMESPACE_LIST))
        .isTrue();

    // Listing directly on NS1 succeeds
    Assertions.assertThat(newWrapper().listNamespaces(NS1).namespaces())
        .containsAll(List.of(NS1A, NS1B));

    // Root listing fails
    Assertions.assertThatThrownBy(() -> newWrapper().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);

    // NS2 listing fails
    Assertions.assertThatThrownBy(() -> newWrapper().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);

    // Listing on a child of NS1 succeeds
    Assertions.assertThat(newWrapper().listNamespaces(NS1A).namespaces())
        .containsAll(List.of(NS1AA));
  }

  @Test
  public void testCreateNamespaceAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DROP))
        .isTrue();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createNamespace(
                  CreateNamespaceRequest.builder().withNamespace(Namespace.of("newns")).build());
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createNamespace(
                  CreateNamespaceRequest.builder()
                      .withNamespace(Namespace.of("ns1", "ns1a", "newns"))
                      .build());
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropNamespace(Namespace.of("newns"));
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropNamespace(Namespace.of("ns1", "ns1a", "newns"));
        });
  }

  @Test
  public void testCreateNamespacesInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_DROP,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_LIST),
        () ->
            newWrapper()
                .createNamespace(
                    CreateNamespaceRequest.builder().withNamespace(Namespace.of("newns")).build()));
  }

  @Test
  public void testLoadNamespaceMetadataSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadNamespaceMetadata(NS1A),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadNamespaceMetadataInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_LIST,
            PolarisPrivilege.NAMESPACE_DROP),
        () -> newWrapper().loadNamespaceMetadata(NS1A));
  }

  @Test
  public void testNamespaceExistsAllSufficientPrivileges() {
    // TODO: If we change the behavior of existence-check to return 404 on unauthorized,
    // the overall test structure will need to change (other tests catching ForbiddenException
    // need to still have catalog-level "REFERENCE" equivalent privileges, and the exists()
    // tests need to expect 404 instead).
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_LIST,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().namespaceExists(NS1A),
        null /* cleanupAction */);
  }

  @Test
  public void testNamespaceExistsInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_DROP),
        () -> newWrapper().namespaceExists(NS1A));
  }

  @Test
  public void testDropNamespaceSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_CREATE))
        .isTrue();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_DROP,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropNamespace(NS1AA);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2))
              .createNamespace(CreateNamespaceRequest.builder().withNamespace(NS1AA).build());
        });
  }

  @Test
  public void testDropNamespaceInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_LIST,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES),
        () -> newWrapper().dropNamespace(NS1AA));
  }

  @Test
  public void testUpdateNamespacePropertiesAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_WRITE_PROPERTIES,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper()
              .updateNamespaceProperties(
                  NS1A, UpdateNamespacePropertiesRequest.builder().update("foo", "bar").build());
          newWrapper()
              .updateNamespaceProperties(
                  NS1A, UpdateNamespacePropertiesRequest.builder().remove("foo").build());
        },
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateNamespacePropertiesInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_LIST,
            PolarisPrivilege.NAMESPACE_READ_PROPERTIES,
            PolarisPrivilege.NAMESPACE_CREATE,
            PolarisPrivilege.NAMESPACE_DROP),
        () ->
            newWrapper()
                .updateNamespaceProperties(
                    NS1A, UpdateNamespacePropertiesRequest.builder().update("foo", "bar").build()));
  }

  @Test
  public void testListTablesAllSufficientPrivileges() {
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
        () -> newWrapper().listTables(NS1A),
        null /* cleanupAction */);
  }

  @Test
  public void testListTablesInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().listTables(NS1A));
  }

  @Test
  public void testCreateTableDirectAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA))
        .isTrue();

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");
    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName("newtable").withSchema(SCHEMA).build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).createTableDirect(NS2, createRequest);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropTableWithPurge(newtable);
        });
  }

  @Test
  public void testCreateTableDirectInsufficientPermissions() {
    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName("newtable").withSchema(SCHEMA).build();

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
          newWrapper(Set.of(PRINCIPAL_ROLE1)).createTableDirect(NS2, createRequest);
        });
  }

  @Test
  public void testCreateTableDirectWithWriteDelegationAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA))
        .isTrue();

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");
    final CreateTableRequest createDirectWithWriteDelegationRequest =
        CreateTableRequest.builder().withName("newtable").withSchema(SCHEMA).stageCreate().build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createTableDirectWithWriteDelegation(NS2, createDirectWithWriteDelegationRequest);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropTableWithPurge(newtable);
        },
        PRINCIPAL_NAME);
  }

  @Test
  public void testCreateTableDirectWithWriteDelegationInsufficientPermissions() {
    final CreateTableRequest createDirectWithWriteDelegationRequest =
        CreateTableRequest.builder()
            .withName("directtable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_CREATE, // TABLE_CREATE itself is insufficient for delegation
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createTableDirectWithWriteDelegation(NS2, createDirectWithWriteDelegationRequest);
        });
  }

  @Test
  public void testCreateTableStagedAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();

    final CreateTableRequest createStagedRequest =
        CreateTableRequest.builder()
            .withName("stagetable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).createTableStaged(NS2, createStagedRequest);
        },
        // createTableStaged doesn't actually commit any metadata
        null);
  }

  @Test
  public void testCreateTableStagedInsufficientPermissions() {
    final CreateTableRequest createStagedRequest =
        CreateTableRequest.builder()
            .withName("stagetable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

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
          newWrapper(Set.of(PRINCIPAL_ROLE1)).createTableStaged(NS2, createStagedRequest);
        });
  }

  @Test
  public void testCreateTableStagedWithWriteDelegationAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();

    final CreateTableRequest createStagedWithWriteDelegationRequest =
        CreateTableRequest.builder()
            .withName("stagetable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createTableStagedWithWriteDelegation(NS2, createStagedWithWriteDelegationRequest);
        },
        // createTableStagedWithWriteDelegation doesn't actually commit any metadata
        null,
        PRINCIPAL_NAME);
  }

  @Test
  public void testCreateTableStagedWithWriteDelegationInsufficientPermissions() {
    final CreateTableRequest createStagedWithWriteDelegationRequest =
        CreateTableRequest.builder()
            .withName("stagetable")
            .withSchema(SCHEMA)
            .stageCreate()
            .build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_CREATE, // TABLE_CREATE itself is insufficient for delegation
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createTableStagedWithWriteDelegation(NS2, createStagedWithWriteDelegationRequest);
        });
  }

  @Test
  public void testRegisterTableAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_READ_PROPERTIES))
        .isTrue();

    // To get a handy metadata file we can use one from another table.
    // to avoid overlapping directories, drop the original table and recreate it via registerTable
    final String metadataLocation = newWrapper().loadTable(TABLE_NS1_1, "all").metadataLocation();
    newWrapper(Set.of(PRINCIPAL_ROLE2)).dropTableWithoutPurge(TABLE_NS1_1);

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
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).registerTable(NS1, registerRequest);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropTableWithoutPurge(TABLE_NS1_1);
        });
  }

  @Test
  public void testRegisterTableInsufficientPermissions() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_READ_PROPERTIES))
        .isTrue();

    // To get a handy metadata file we can use one from another table.
    final String metadataLocation = newWrapper().loadTable(TABLE_NS1_1, "all").metadataLocation();

    final RegisterTableRequest registerRequest =
        new RegisterTableRequest() {
          @Override
          public String name() {
            return "newtable";
          }

          @Override
          public String metadataLocation() {
            return metadataLocation;
          }
        };

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
          newWrapper(Set.of(PRINCIPAL_ROLE1)).registerTable(NS2, registerRequest);
        });
  }

  @Test
  public void testLoadTableSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadTable(TABLE_NS1A_2, "all"),
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
        () -> newWrapper().loadTable(TABLE_NS1A_2, "all"));
  }

  @Test
  public void testLoadTableWithReadAccessDelegationSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all"),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadTableWithReadAccessDelegationInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all"));
  }

  @Test
  public void testLoadTableWithWriteAccessDelegationSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            // TODO: Once we give different creds for read/write privilege, move this
            // TABLE_READ_DATA into a special-case test; with only TABLE_READ_DATA we'd expet
            // to receive a read-only credential.
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all"),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadTableWithWriteAccessDelegationInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all"));
  }

  @Test
  public void testUpdateTableSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, new UpdateTableRequest()),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().updateTable(TABLE_NS1A_2, new UpdateTableRequest()));
  }

  @Test
  public void testUpdateTableForStagedCreateSufficientPrivileges() {
    // Note: This is kind of cheating by only leaning on the PolarisCatalogHandlerWrapper level
    // of differentiation between updateForStageCreate vs regular update so that we don't need
    // to actually set up the staged create but still test the privileges. If the underlying
    // behavior diverges, we need to change this test to actually start with a stageCreate.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTableForStagedCreate(TABLE_NS1A_2, new UpdateTableRequest()),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableForStagedCreateInsufficientPermissions() {
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
        () -> newWrapper().updateTableForStagedCreate(TABLE_NS1A_2, new UpdateTableRequest()));
  }

  @Test
  public void testDropTableWithoutPurgeAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE))
        .isTrue();

    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName(TABLE_NS1_1.name()).withSchema(SCHEMA).build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropTableWithoutPurge(TABLE_NS1_1);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2))
              .createTableDirect(TABLE_NS1_1.namespace(), createRequest);
        });
  }

  @Test
  public void testDropTableWithoutPurgeInsufficientPermissions() {
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
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropTableWithoutPurge(TABLE_NS1_1);
        });
  }

  @Test
  public void testDropTableWithPurgeAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE))
        .isTrue();

    final CreateTableRequest createRequest =
        CreateTableRequest.builder().withName(TABLE_NS1_1.name()).withSchema(SCHEMA).build();

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_FULL_METADATA),
            Set.of(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_DROP),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropTableWithPurge(TABLE_NS1_1);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2))
              .createTableDirect(TABLE_NS1_1.namespace(), createRequest);
        },
        PRINCIPAL_NAME);
  }

  @Test
  public void testDropTableWithPurgeInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP, // TABLE_DROP itself is insufficient for purge
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropTableWithPurge(TABLE_NS1_1);
        });
  }

  @Test
  public void testTableExistsSufficientPrivileges() {
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
        () -> newWrapper().tableExists(TABLE_NS1A_2),
        null /* cleanupAction */);
  }

  @Test
  public void testTableExistsInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().tableExists(TABLE_NS1A_2));
  }

  @Test
  public void testRenameTableAllSufficientPrivileges() {
    final TableIdentifier srcTable = TABLE_NS1_1;
    final TableIdentifier dstTable = TableIdentifier.of(NS1AA, "newtable");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcTable).withDestination(dstTable).build();
    final RenameTableRequest rename2 =
        RenameTableRequest.builder().withSource(dstTable).withDestination(srcTable).build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.TABLE_FULL_METADATA),
            Set.of(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_DROP),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).renameTable(rename1);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).renameTable(rename2);
        },
        PRINCIPAL_NAME);
  }

  @Test
  public void testRenameTableInsufficientPermissions() {
    final TableIdentifier srcTable = TABLE_NS1_1;
    final TableIdentifier dstTable = TableIdentifier.of(NS1AA, "newtable");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcTable).withDestination(dstTable).build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_DROP,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).renameTable(rename1);
        });
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
    Assertions.assertThat(
            adminService.grantPrivilegeOnTableToRole(
                CATALOG_NAME, CATALOG_ROLE1, srcTable, PolarisPrivilege.TABLE_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, dstTable.namespace(), PolarisPrivilege.TABLE_CREATE))
        .isTrue();

    // Initial rename should succeed
    newWrapper().renameTable(rename1);

    // Inverse operation should fail
    Assertions.assertThatThrownBy(() -> newWrapper().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Now grant TABLE_DROP on dst
    Assertions.assertThat(
            adminService.grantPrivilegeOnTableToRole(
                CATALOG_NAME, CATALOG_ROLE1, dstTable, PolarisPrivilege.TABLE_DROP))
        .isTrue();

    // Still not enough without TABLE_CREATE at source
    Assertions.assertThatThrownBy(() -> newWrapper().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Even grant CATALOG_MANAGE_CONTENT under all of NS1
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.CATALOG_MANAGE_CONTENT))
        .isTrue();

    // Still not enough to rename back to src since src was NS2.
    Assertions.assertThatThrownBy(() -> newWrapper().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Finally, grant TABLE_CREATE on NS2 and it should succeed to rename back to src.
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS2, PolarisPrivilege.TABLE_CREATE))
        .isTrue();
    newWrapper().renameTable(rename2);
  }

  @Test
  public void testCommitTransactionSufficientPrivileges() {
    CommitTransactionRequest req =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(TABLE_NS1_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1A_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1B_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS2_1, List.of(), List.of())));

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT),
            Set.of(PolarisPrivilege.TABLE_FULL_METADATA),
            Set.of(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA),
            Set.of(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_PROPERTIES)),
        () -> newWrapper().commitTransaction(req),
        null,
        PRINCIPAL_NAME /* cleanupAction */);
  }

  @Test
  public void testCommitTransactionInsufficientPermissions() {
    CommitTransactionRequest req =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(TABLE_NS1_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1A_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS1B_1, List.of(), List.of()),
                UpdateTableRequest.create(TABLE_NS2_1, List.of(), List.of())));

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () -> newWrapper().commitTransaction(req));
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
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.TABLE_CREATE))
        .isTrue();
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_FULL_METADATA directly on TABLE_NS1_1
    Assertions.assertThat(
            adminService.grantPrivilegeOnTableToRole(
                CATALOG_NAME, CATALOG_ROLE1, TABLE_NS1_1, PolarisPrivilege.TABLE_FULL_METADATA))
        .isTrue();
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_PROPERTIES on NS1A namespace
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS1A, PolarisPrivilege.TABLE_WRITE_PROPERTIES))
        .isTrue();
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_DATA directly on TABLE_NS1B_1
    Assertions.assertThat(
            adminService.grantPrivilegeOnTableToRole(
                CATALOG_NAME, CATALOG_ROLE1, TABLE_NS1B_1, PolarisPrivilege.TABLE_WRITE_DATA))
        .isTrue();
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_PROPERTIES directly on TABLE_NS2_1
    Assertions.assertThat(
            adminService.grantPrivilegeOnTableToRole(
                CATALOG_NAME, CATALOG_ROLE1, TABLE_NS2_1, PolarisPrivilege.TABLE_WRITE_PROPERTIES))
        .isTrue();
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Also grant TABLE_CREATE directly on TABLE_NS2_1
    // TODO: If we end up having fine-grained differentiation between updateForStagedCreate
    // and update, then this one should only be TABLE_CREATE on the *parent* of this last table
    // and the table shouldn't exist.
    Assertions.assertThat(
            adminService.grantPrivilegeOnTableToRole(
                CATALOG_NAME, CATALOG_ROLE1, TABLE_NS2_1, PolarisPrivilege.TABLE_CREATE))
        .isTrue();
    newWrapper().commitTransaction(req);
  }

  @Test
  public void testListViewsAllSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.VIEW_LIST,
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().listViews(NS1A),
        null /* cleanupAction */);
  }

  @Test
  public void testListViewsInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_DROP),
        () -> newWrapper().listViews(NS1A));
  }

  @Test
  public void testCreateViewAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.VIEW_DROP))
        .isTrue();

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

    // Use PRINCIPAL_ROLE1 for privilege-testing, PRINCIPAL_ROLE2 for cleanup.
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).createView(NS2, createRequest);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).dropView(newview);
        });
  }

  @Test
  public void testCreateViewInsufficientPermissions() {
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

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_DROP,
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).createView(NS2, createRequest);
        });
  }

  @Test
  public void testLoadViewSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadView(VIEW_NS1A_2),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadViewInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_LIST,
            PolarisPrivilege.VIEW_DROP),
        () -> newWrapper().loadView(VIEW_NS1A_2));
  }

  @Test
  public void testUpdateViewSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().replaceView(VIEW_NS1A_2, new UpdateTableRequest()),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateViewInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_LIST,
            PolarisPrivilege.VIEW_DROP),
        () -> newWrapper().replaceView(VIEW_NS1A_2, new UpdateTableRequest()));
  }

  @Test
  public void testDropViewAllSufficientPrivileges() {
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.VIEW_CREATE))
        .isTrue();

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
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.VIEW_DROP,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropView(VIEW_NS1_1);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2)).createView(VIEW_NS1_1.namespace(), createRequest);
        });
  }

  @Test
  public void testDropViewInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).dropView(VIEW_NS1_1);
        });
  }

  @Test
  public void testViewExistsSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.VIEW_LIST,
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().viewExists(VIEW_NS1A_2),
        null /* cleanupAction */);
  }

  @Test
  public void testViewExistsInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_DROP),
        () -> newWrapper().viewExists(VIEW_NS1A_2));
  }

  @Test
  public void testRenameViewAllSufficientPrivileges() {
    final TableIdentifier srcView = VIEW_NS1_1;
    final TableIdentifier dstView = TableIdentifier.of(NS1AA, "newview");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcView).withDestination(dstView).build();
    final RenameTableRequest rename2 =
        RenameTableRequest.builder().withSource(dstView).withDestination(srcView).build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.VIEW_FULL_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT),
            Set.of(PolarisPrivilege.VIEW_DROP, PolarisPrivilege.VIEW_CREATE)),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).renameView(rename1);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).renameView(rename2);
        },
        PRINCIPAL_NAME);
  }

  @Test
  public void testRenameViewInsufficientPermissions() {
    final TableIdentifier srcView = VIEW_NS1_1;
    final TableIdentifier dstView = TableIdentifier.of(NS1AA, "newview");
    final RenameTableRequest rename1 =
        RenameTableRequest.builder().withSource(srcView).withDestination(dstView).build();

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_DROP,
            PolarisPrivilege.VIEW_CREATE,
            PolarisPrivilege.VIEW_READ_PROPERTIES,
            PolarisPrivilege.VIEW_WRITE_PROPERTIES,
            PolarisPrivilege.VIEW_LIST),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).renameView(rename1);
        });
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
    Assertions.assertThat(
            adminService.grantPrivilegeOnViewToRole(
                CATALOG_NAME, CATALOG_ROLE1, srcView, PolarisPrivilege.VIEW_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, dstView.namespace(), PolarisPrivilege.VIEW_CREATE))
        .isTrue();

    // Initial rename should succeed
    newWrapper().renameView(rename1);

    // Inverse operation should fail
    Assertions.assertThatThrownBy(() -> newWrapper().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Now grant VIEW_DROP on dst
    Assertions.assertThat(
            adminService.grantPrivilegeOnViewToRole(
                CATALOG_NAME, CATALOG_ROLE1, dstView, PolarisPrivilege.VIEW_DROP))
        .isTrue();

    // Still not enough without VIEW_CREATE at source
    Assertions.assertThatThrownBy(() -> newWrapper().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Even grant CATALOG_MANAGE_CONTENT under all of NS1
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.CATALOG_MANAGE_CONTENT))
        .isTrue();

    // Still not enough to rename back to src since src was NS2.
    Assertions.assertThatThrownBy(() -> newWrapper().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Finally, grant VIEW_CREATE on NS2 and it should succeed to rename back to src.
    Assertions.assertThat(
            adminService.grantPrivilegeOnNamespaceToRole(
                CATALOG_NAME, CATALOG_ROLE1, NS2, PolarisPrivilege.VIEW_CREATE))
        .isTrue();
    newWrapper().renameView(rename2);
  }

  @Test
  public void testSendNotificationSufficientPrivileges() {
    String externalCatalog = "externalCatalog";
    String storageLocation =
        "file:///tmp/send_notification_sufficient_privileges_" + System.currentTimeMillis();

    FileStorageConfigInfo storageConfigModel =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    adminService.createCatalog(
        new CatalogEntity.Builder()
            .setName(externalCatalog)
            .setDefaultBaseLocation(storageLocation)
            .setStorageConfigurationInfo(storageConfigModel, storageLocation)
            .setCatalogType("EXTERNAL")
            .build());
    adminService.createCatalogRole(
        externalCatalog, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE1).build());
    adminService.createCatalogRole(
        externalCatalog, new CatalogRoleEntity.Builder().setName(CATALOG_ROLE2).build());

    adminService.assignPrincipalRole(PRINCIPAL_NAME, PRINCIPAL_ROLE1);
    adminService.assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE1, externalCatalog, CATALOG_ROLE1);
    adminService.assignCatalogRoleToPrincipalRole(PRINCIPAL_ROLE2, externalCatalog, CATALOG_ROLE2);
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                externalCatalog, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP))
        .isTrue();
    Assertions.assertThat(
            adminService.grantPrivilegeOnCatalogToRole(
                externalCatalog, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DROP))
        .isTrue();

    Namespace namespace = Namespace.of("extns1", "extns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");

    String tableUuid = UUID.randomUUID().toString();

    NotificationRequest createRequest = new NotificationRequest();
    createRequest.setNotificationType(NotificationType.CREATE);
    TableUpdateNotification createPayload = new TableUpdateNotification();
    createPayload.setMetadataLocation(
        String.format("%s/bucket/table/metadata/v1.metadata.json", storageLocation));
    createPayload.setTableName(table.name());
    createPayload.setTableUuid(tableUuid);
    createPayload.setTimestamp(230950845L);
    createRequest.setPayload(createPayload);

    NotificationRequest updateRequest = new NotificationRequest();
    updateRequest.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification updatePayload = new TableUpdateNotification();
    updatePayload.setMetadataLocation(
        String.format("%s/bucket/table/metadata/v2.metadata.json", storageLocation));
    updatePayload.setTableName(table.name());
    updatePayload.setTableUuid(tableUuid);
    updatePayload.setTimestamp(330950845L);
    updateRequest.setPayload(updatePayload);

    NotificationRequest dropRequest = new NotificationRequest();
    dropRequest.setNotificationType(NotificationType.DROP);
    TableUpdateNotification dropPayload = new TableUpdateNotification();
    dropPayload.setTableName(table.name());
    dropPayload.setTableUuid(tableUuid);
    dropPayload.setTimestamp(430950845L);
    dropRequest.setPayload(dropPayload);

    NotificationRequest validateRequest = new NotificationRequest();
    validateRequest.setNotificationType(NotificationType.VALIDATE);
    TableUpdateNotification validatePayload = new TableUpdateNotification();
    validatePayload.setMetadataLocation(
        String.format("%s/bucket/table/metadata/v1.metadata.json", storageLocation));
    validatePayload.setTableName(table.name());
    validatePayload.setTableUuid(tableUuid);
    validatePayload.setTimestamp(530950845L);
    validateRequest.setPayload(validatePayload);

    PolarisCallContextCatalogFactory factory =
        new PolarisCallContextCatalogFactory(
            new RealmEntityManagerFactory() {
              @Override
              public PolarisEntityManager getOrCreateEntityManager(RealmContext realmContext) {
                return entityManager;
              }
            },
            metaStoreManagerFactory,
            Mockito.mock(),
            new DefaultFileIOFactory()) {
          @Override
          public Catalog createCallContextCatalog(
              CallContext context,
              AuthenticatedPolarisPrincipal authenticatedPolarisPrincipal,
              PolarisResolutionManifest resolvedManifest) {
            Catalog catalog =
                super.createCallContextCatalog(
                    context, authenticatedPolarisPrincipal, resolvedManifest);
            String fileIoImpl = "org.apache.iceberg.inmemory.InMemoryFileIO";
            catalog.initialize(
                externalCatalog, ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, fileIoImpl));

            FileIO fileIO = CatalogUtil.loadFileIO(fileIoImpl, Map.of(), new Configuration());
            TableMetadata tableMetadata =
                TableMetadata.buildFromEmpty()
                    .addSchema(SCHEMA, SCHEMA.highestFieldId())
                    .setLocation(
                        String.format("%s/bucket/table/metadata/v1.metadata.json", storageLocation))
                    .addPartitionSpec(PartitionSpec.unpartitioned())
                    .addSortOrder(SortOrder.unsorted())
                    .assignUUID()
                    .build();
            TableMetadataParser.overwrite(
                tableMetadata, fileIO.newOutputFile(createPayload.getMetadataLocation()));
            TableMetadataParser.overwrite(
                tableMetadata, fileIO.newOutputFile(updatePayload.getMetadataLocation()));
            return catalog;
          }
        };

    List<Set<PolarisPrivilege>> sufficientPrivilegeSets =
        List.of(
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT),
            Set.of(PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.NAMESPACE_FULL_METADATA),
            Set.of(
                PolarisPrivilege.TABLE_FULL_METADATA,
                PolarisPrivilege.NAMESPACE_CREATE,
                PolarisPrivilege.NAMESPACE_DROP),
            Set.of(
                PolarisPrivilege.TABLE_CREATE,
                PolarisPrivilege.TABLE_DROP,
                PolarisPrivilege.TABLE_WRITE_PROPERTIES,
                PolarisPrivilege.NAMESPACE_FULL_METADATA),
            Set.of(
                PolarisPrivilege.TABLE_CREATE,
                PolarisPrivilege.TABLE_DROP,
                PolarisPrivilege.TABLE_WRITE_PROPERTIES,
                PolarisPrivilege.NAMESPACE_CREATE,
                PolarisPrivilege.NAMESPACE_DROP));
    doTestSufficientPrivilegeSets(
        sufficientPrivilegeSets,
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
              .sendNotification(table, createRequest);
          newWrapper(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
              .sendNotification(table, updateRequest);
          newWrapper(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
              .sendNotification(table, dropRequest);
          newWrapper(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
              .sendNotification(table, validateRequest);
        },
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
              .dropNamespace(Namespace.of("extns1", "extns2"));
          newWrapper(Set.of(PRINCIPAL_ROLE2), externalCatalog, factory)
              .dropNamespace(Namespace.of("extns1"));
        },
        PRINCIPAL_NAME,
        externalCatalog);

    // Also test VALIDATE in isolation
    doTestSufficientPrivilegeSets(
        sufficientPrivilegeSets,
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1), externalCatalog, factory)
              .sendNotification(table, validateRequest);
        },
        null /* cleanupAction */,
        PRINCIPAL_NAME,
        externalCatalog);
  }

  @Test
  public void testSendNotificationInsufficientPermissions() {
    Namespace namespace = Namespace.of("ns1", "ns2");
    TableIdentifier table = TableIdentifier.of(namespace, "tbl1");

    NotificationRequest request = new NotificationRequest();
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation("file:///tmp/bucket/table/metadata/v1.metadata.json");
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    List<PolarisPrivilege> insufficientPrivileges =
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA);

    // Independently test insufficient privileges in isolation.
    request.setNotificationType(NotificationType.CREATE);
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).sendNotification(table, request);
        });

    request.setNotificationType(NotificationType.UPDATE);
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).sendNotification(table, request);
        });

    request.setNotificationType(NotificationType.DROP);
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).sendNotification(table, request);
        });

    request.setNotificationType(NotificationType.VALIDATE);
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1)).sendNotification(table, request);
        });
  }
}
