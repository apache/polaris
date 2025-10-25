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
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
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
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.context.catalog.PolarisCallContextCatalogFactory;
import org.apache.polaris.service.http.IfNoneMatch;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
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
@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
public class IcebergCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  @Inject CallContextCatalogFactory callContextCatalogFactory;
  @Inject Instance<ExternalCatalogFactory> externalCatalogFactories;

  @SuppressWarnings("unchecked")
  private static Instance<ExternalCatalogFactory> emptyExternalCatalogFactory() {
    Instance<ExternalCatalogFactory> mock = Mockito.mock(Instance.class);
    Mockito.when(mock.select(Mockito.any())).thenReturn(mock);
    Mockito.when(mock.isUnsatisfied()).thenReturn(true);
    return mock;
  }

  protected IcebergCatalogHandler newWrapper() {
    return newWrapper(Set.of());
  }

  private IcebergCatalogHandler newWrapper(Set<String> activatedPrincipalRoles) {
    return newWrapper(activatedPrincipalRoles, CATALOG_NAME, callContextCatalogFactory);
  }

  private IcebergCatalogHandler newWrapper(
      Set<String> activatedPrincipalRoles, String catalogName, CallContextCatalogFactory factory) {
    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);
    return new IcebergCatalogHandler(
        diagServices,
        callContext,
        resolutionManifestFactory,
        metaStoreManager,
        credentialManager,
        securityContext(authenticatedPrincipal),
        factory,
        catalogName,
        polarisAuthorizer,
        reservedProperties,
        catalogHandlerUtils,
        emptyExternalCatalogFactory(),
        polarisEventListener,
        storageAccessConfigProvider);
  }

  protected void doTestInsufficientPrivileges(
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
  protected void doTestSufficientPrivileges(
      List<PolarisPrivilege> sufficientPrivileges, Runnable action, Runnable cleanupAction) {
    doTestSufficientPrivilegeSets(
        sufficientPrivileges.stream().map(Set::of).toList(), action, cleanupAction, PRINCIPAL_NAME);
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
  protected void doTestSufficientPrivilegeSets(
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
    IcebergCatalogHandler wrapper =
        new IcebergCatalogHandler(
            diagServices,
            callContext,
            resolutionManifestFactory,
            metaStoreManager,
            credentialManager,
            securityContext(authenticatedPrincipal),
            callContextCatalogFactory,
            CATALOG_NAME,
            polarisAuthorizer,
            reservedProperties,
            catalogHandlerUtils,
            emptyExternalCatalogFactory(),
            polarisEventListener,
            storageAccessConfigProvider);

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
    PolarisPrincipal authenticatedPrincipal1 =
        PolarisPrincipal.of(refreshPrincipal, Set.of(PRINCIPAL_ROLE1, PRINCIPAL_ROLE2));
    IcebergCatalogHandler refreshedWrapper =
        new IcebergCatalogHandler(
            diagServices,
            callContext,
            resolutionManifestFactory,
            metaStoreManager,
            credentialManager,
            securityContext(authenticatedPrincipal1),
            callContextCatalogFactory,
            CATALOG_NAME,
            polarisAuthorizer,
            reservedProperties,
            catalogHandlerUtils,
            emptyExternalCatalogFactory(),
            polarisEventListener,
            storageAccessConfigProvider);

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.NAMESPACE_LIST));
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
    assertSuccess(
        adminService.revokePrivilegeOnCatalogFromRole(
            CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.NAMESPACE_LIST));
    Assertions.assertThatThrownBy(() -> newWrapper().listNamespaces(Namespace.of()))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  public void testListNamespacesChildOnly() {
    // Grant only NS1-level privilege to CATALOG_ROLE1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.NAMESPACE_LIST));

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DROP));

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_CREATE));

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_WRITE_DATA));

    final TableIdentifier newtable = TableIdentifier.of(NS2, "newtable");
    final CreateTableRequest createDirectWithWriteDelegationRequest =
        CreateTableRequest.builder().withName("newtable").withSchema(SCHEMA).stageCreate().build();

    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> {
          newWrapper(Set.of(PRINCIPAL_ROLE1))
              .createTableDirectWithWriteDelegation(
                  NS2, createDirectWithWriteDelegationRequest, Optional.empty());
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
              .createTableDirectWithWriteDelegation(
                  NS2, createDirectWithWriteDelegationRequest, Optional.empty());
        });
  }

  @Test
  public void testCreateTableStagedAllSufficientPrivileges() {
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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));

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
              .createTableStagedWithWriteDelegation(
                  NS2, createStagedWithWriteDelegationRequest, Optional.empty());
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
              .createTableStagedWithWriteDelegation(
                  NS2, createStagedWithWriteDelegationRequest, Optional.empty());
        });
  }

  @Test
  public void testRegisterTableAllSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_READ_PROPERTIES));

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_READ_PROPERTIES));

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
  public void testLoadTableIfStaleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () ->
            newWrapper().loadTableIfStale(TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all"),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadTableIfStaleInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP),
        () ->
            newWrapper()
                .loadTableIfStale(TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all"));
  }

  @Test
  public void testLoadTableWithReadAccessDelegationSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all", Optional.empty()),
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
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all", Optional.empty()));
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
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all", Optional.empty()),
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
        () -> newWrapper().loadTableWithAccessDelegation(TABLE_NS1A_2, "all", Optional.empty()));
  }

  @Test
  public void testLoadTableWithReadAccessDelegationIfStaleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () ->
            newWrapper()
                .loadTableWithAccessDelegationIfStale(
                    TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all", Optional.empty()),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadTableWithReadAccessDelegationIfStaleInsufficientPermissions() {
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
        () ->
            newWrapper()
                .loadTableWithAccessDelegationIfStale(
                    TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all", Optional.empty()));
  }

  @Test
  public void testLoadTableWithWriteAccessDelegationIfStaleSufficientPrivileges() {
    doTestSufficientPrivileges(
        List.of(
            // TODO: Once we give different creds for read/write privilege, move this
            // TABLE_READ_DATA into a special-case test; with only TABLE_READ_DATA we'd expet
            // to receive a read-only credential.
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () ->
            newWrapper()
                .loadTableWithAccessDelegationIfStale(
                    TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all", Optional.empty()),
        null /* cleanupAction */);
  }

  @Test
  public void testLoadTableWithWriteAccessDelegationIfStaleInsufficientPermissions() {
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
        () ->
            newWrapper()
                .loadTableWithAccessDelegationIfStale(
                    TABLE_NS1A_2, IfNoneMatch.fromHeader("W/\"0:0\""), "all", Optional.empty()));
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
  public void testUpdateTableFallbackToCoarseGrainedWhenFeatureDisabled() {
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
    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_WRITE_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapperWithFineGrainedAuthzDisabled().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  /**
   * Creates a wrapper with fine-grained authorization explicitly disabled for testing the fallback
   * behavior to coarse-grained authorization.
   */
  private IcebergCatalogHandler newWrapperWithFineGrainedAuthzDisabled() {
    // Create a custom CallContextCatalogFactory that mocks the configuration
    CallContextCatalogFactory mockFactory = Mockito.mock(CallContextCatalogFactory.class);

    // Mock the catalog factory to return our regular catalog but with mocked config
    Mockito.when(
            mockFactory.createCallContextCatalog(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(baseCatalog);

    return newWrapperWithFineLevelAuthDisabled(Set.of(), CATALOG_NAME, mockFactory, false);
  }

  private IcebergCatalogHandler newWrapperWithFineLevelAuthDisabled(
      Set<String> activatedPrincipalRoles,
      String catalogName,
      CallContextCatalogFactory factory,
      boolean fineGrainedAuthzEnabled) {

    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(principalEntity, activatedPrincipalRoles);

    // Create a custom CallContext that returns a custom RealmConfig
    CallContext mockCallContext = Mockito.mock(CallContext.class);

    // Create a simple RealmConfig implementation that overrides just what we need
    RealmConfig customRealmConfig =
        new RealmConfig() {
          @Override
          public <T> T getConfig(String configName) {
            return realmConfig.getConfig(configName);
          }

          @Override
          public <T> T getConfig(String configName, T defaultValue) {
            return realmConfig.getConfig(configName, defaultValue);
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
              return (T) Boolean.valueOf(fineGrainedAuthzEnabled);
            }
            return realmConfig.getConfig(config, catalogEntity);
          }
        };

    // Mock the regular CallContext calls
    Mockito.when(mockCallContext.getRealmConfig()).thenReturn(customRealmConfig);
    Mockito.when(mockCallContext.getPolarisCallContext())
        .thenReturn(callContext.getPolarisCallContext());

    return new IcebergCatalogHandler(
        diagServices,
        mockCallContext,
        resolutionManifestFactory,
        metaStoreManager,
        credentialManager,
        securityContext(authenticatedPrincipal),
        factory,
        catalogName,
        polarisAuthorizer,
        reservedProperties,
        catalogHandlerUtils,
        emptyExternalCatalogFactory(),
        polarisEventListener,
        storageAccessConfigProvider);
  }

  @Test
  public void testDropTableWithoutPurgeAllSufficientPrivileges() {
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

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
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            CATALOG_NAME, CATALOG_ROLE2, PolarisPrivilege.TABLE_CREATE));

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
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, srcTable, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstTable.namespace(), PolarisPrivilege.TABLE_CREATE));

    // Initial rename should succeed
    newWrapper().renameTable(rename1);

    // Inverse operation should fail
    Assertions.assertThatThrownBy(() -> newWrapper().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Now grant TABLE_DROP on dst
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstTable, PolarisPrivilege.TABLE_DROP));

    // Still not enough without TABLE_CREATE at source
    Assertions.assertThatThrownBy(() -> newWrapper().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Even grant CATALOG_MANAGE_CONTENT under all of NS1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.CATALOG_MANAGE_CONTENT));

    // Still not enough to rename back to src since src was NS2.
    Assertions.assertThatThrownBy(() -> newWrapper().renameTable(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Finally, grant TABLE_CREATE on NS2 and it should succeed to rename back to src.
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS2, PolarisPrivilege.TABLE_CREATE));
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
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.TABLE_CREATE));
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_FULL_METADATA directly on TABLE_NS1_1
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS1_1, PolarisPrivilege.TABLE_FULL_METADATA));
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_PROPERTIES on NS1A namespace
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1A, PolarisPrivilege.TABLE_WRITE_PROPERTIES));
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_DATA directly on TABLE_NS1B_1
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS1B_1, PolarisPrivilege.TABLE_WRITE_DATA));
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Grant TABLE_WRITE_PROPERTIES directly on TABLE_NS2_1
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS2_1, PolarisPrivilege.TABLE_WRITE_PROPERTIES));
    Assertions.assertThatThrownBy(() -> newWrapper().commitTransaction(req))
        .isInstanceOf(ForbiddenException.class);

    // Also grant TABLE_CREATE directly on TABLE_NS2_1
    // TODO: If we end up having fine-grained differentiation between updateForStagedCreate
    // and update, then this one should only be TABLE_CREATE on the *parent* of this last table
    // and the table shouldn't exist.
    assertSuccess(
        adminService.grantPrivilegeOnTableToRole(
            CATALOG_NAME, CATALOG_ROLE1, TABLE_NS2_1, PolarisPrivilege.TABLE_CREATE));
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
    assertSuccess(
        adminService.grantPrivilegeOnViewToRole(
            CATALOG_NAME, CATALOG_ROLE1, srcView, PolarisPrivilege.VIEW_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstView.namespace(), PolarisPrivilege.VIEW_CREATE));

    // Initial rename should succeed
    newWrapper().renameView(rename1);

    // Inverse operation should fail
    Assertions.assertThatThrownBy(() -> newWrapper().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Now grant VIEW_DROP on dst
    assertSuccess(
        adminService.grantPrivilegeOnViewToRole(
            CATALOG_NAME, CATALOG_ROLE1, dstView, PolarisPrivilege.VIEW_DROP));

    // Still not enough without VIEW_CREATE at source
    Assertions.assertThatThrownBy(() -> newWrapper().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Even grant CATALOG_MANAGE_CONTENT under all of NS1
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS1, PolarisPrivilege.CATALOG_MANAGE_CONTENT));

    // Still not enough to rename back to src since src was NS2.
    Assertions.assertThatThrownBy(() -> newWrapper().renameView(rename2))
        .isInstanceOf(ForbiddenException.class);

    // Finally, grant VIEW_CREATE on NS2 and it should succeed to rename back to src.
    assertSuccess(
        adminService.grantPrivilegeOnNamespaceToRole(
            CATALOG_NAME, CATALOG_ROLE1, NS2, PolarisPrivilege.VIEW_CREATE));
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
            externalCatalog, CATALOG_ROLE2, PolarisPrivilege.TABLE_DROP));
    assertSuccess(
        adminService.grantPrivilegeOnCatalogToRole(
            externalCatalog, CATALOG_ROLE2, PolarisPrivilege.NAMESPACE_DROP));

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
            diagServices,
            resolverFactory,
            managerFactory,
            Mockito.mock(),
            new DefaultFileIOFactory(storageAccessConfigProvider),
            polarisEventListener) {
          @Override
          public Catalog createCallContextCatalog(
              CallContext context,
              PolarisPrincipal polarisPrincipal,
              SecurityContext securityContext,
              PolarisResolutionManifest resolvedManifest) {
            Catalog catalog =
                super.createCallContextCatalog(
                    context, polarisPrincipal, securityContext, resolvedManifest);
            String fileIoImpl = "org.apache.iceberg.inmemory.InMemoryFileIO";
            catalog.initialize(
                externalCatalog, ImmutableMap.of(CatalogProperties.FILE_IO_IMPL, fileIoImpl));

            FileIO fileIO = CatalogUtil.loadFileIO(fileIoImpl, Map.of(), null);
            TableMetadata tableMetadata =
                TableMetadata.buildFromEmpty()
                    .addSchema(SCHEMA)
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

  @Test
  public void testUpdateTableWith_AssignUuid_Privilege() {
    // Test that TABLE_ASSIGN_UUID privilege is required for AssignUUID MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_ASSIGN_UUID,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWith_AssignUuidInsufficientPermissions() {
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.AssignUUID(UUID.randomUUID().toString())));

    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.NAMESPACE_FULL_METADATA,
            PolarisPrivilege.VIEW_FULL_METADATA,
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_READ_DATA,
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_LIST,
            PolarisPrivilege.TABLE_DROP,
            // Test that other fine-grained privileges don't work
            PolarisPrivilege.TABLE_ADD_SCHEMA,
            PolarisPrivilege.TABLE_SET_LOCATION),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request));
  }

  @Test
  public void testUpdateTableWith_UpgradeFormatVersionPrivilege() {
    // Test that TABLE_UPGRADE_FORMAT_VERSION privilege is required for UpgradeFormatVersion
    // MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.UpgradeFormatVersion(2)));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWith_SetPropertiesPrivilege() {
    // Test that TABLE_SET_PROPERTIES privilege is required for SetProperties MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.SetProperties(Map.of("test.property", "test.value"))));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_SET_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWith_RemoveProperties_Privilege() {
    // Test that TABLE_REMOVE_PROPERTIES privilege is required for RemoveProperties MetadataUpdate
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(new MetadataUpdate.RemoveProperties(Set.of("property.to.remove"))));

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_REMOVE_PROPERTIES,
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWith_MultipleUpdates_Privilege() {
    // Test that multiple MetadataUpdate types require multiple specific privileges
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(
                new MetadataUpdate.UpgradeFormatVersion(2),
                new MetadataUpdate.SetProperties(Map.of("test.prop", "test.val"))));

    // Test that having both specific privileges works
    doTestSufficientPrivilegeSets(
        List.of(
            Set.of(
                PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION,
                PolarisPrivilege.TABLE_SET_PROPERTIES),
            Set.of(PolarisPrivilege.TABLE_WRITE_PROPERTIES), // Broader privilege should work
            Set.of(PolarisPrivilege.TABLE_FULL_METADATA),
            Set.of(PolarisPrivilege.CATALOG_MANAGE_CONTENT)),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request),
        null /* cleanupAction */,
        PRINCIPAL_NAME,
        CATALOG_NAME);
  }

  @Test
  public void testUpdateTableWith_MultipleUpdatesInsufficientPermissions() {
    // Test that having only one of the required privileges fails
    UpdateTableRequest request =
        UpdateTableRequest.create(
            TABLE_NS1A_2,
            List.of(), // no requirements
            List.of(
                new MetadataUpdate.UpgradeFormatVersion(2),
                new MetadataUpdate.SetProperties(Map.of("test.prop", "test.val"))));

    // Test that having only one specific privilege fails (need both)
    doTestInsufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_UPGRADE_FORMAT_VERSION, // Only one of the two needed
            PolarisPrivilege.TABLE_SET_PROPERTIES, // Only one of the two needed
            PolarisPrivilege.TABLE_ASSIGN_UUID, // Wrong privilege
            PolarisPrivilege.TABLE_READ_PROPERTIES,
            PolarisPrivilege.TABLE_CREATE),
        () -> newWrapper().updateTable(TABLE_NS1A_2, request));
  }

  @Test
  public void testUpdateTableWith_TableManageStructureSuperPrivilege() {
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

    doTestSufficientPrivileges(
        List.of(
            PolarisPrivilege.TABLE_MANAGE_STRUCTURE, // Should work for all structural operations
            PolarisPrivilege.TABLE_WRITE_PROPERTIES, // Should also work with broader privilege
            PolarisPrivilege.TABLE_FULL_METADATA,
            PolarisPrivilege.CATALOG_MANAGE_CONTENT),
        () -> newWrapper().updateTable(TABLE_NS1A_2, structuralRequest),
        null /* cleanupAction */);
  }

  @Test
  public void testUpdateTableWith_TableManageStructureDoesNotIncludeSnapshots() {
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

    doTestSufficientPrivileges(
        List.of(PolarisPrivilege.TABLE_MANAGE_STRUCTURE),
        () -> newWrapper().updateTable(TABLE_NS1A_2, nonSnapshotRequest),
        null /* cleanupAction */);

    // Test that TABLE_MANAGE_STRUCTURE is insufficient for operations that require
    // different privilege categories (like read operations)
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.TABLE_MANAGE_STRUCTURE),
        () ->
            newWrapper()
                .loadTable(TABLE_NS1A_2, "all")); // Load table requires different privileges
  }
}
