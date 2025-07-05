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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.polaris.core.entity.EntityConverter.toCatalog;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.azure.core.exception.HttpResponseException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.TaskEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.InMemoryEntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.CatalogHandlerUtils;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.catalog.io.ExceptionMappingFileIO;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.MeasuredFileIOFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.AfterTableCommitedEvent;
import org.apache.polaris.service.events.AfterTableRefreshedEvent;
import org.apache.polaris.service.events.BeforeTableCommitedEvent;
import org.apache.polaris.service.events.BeforeTableRefreshedEvent;
import org.apache.polaris.service.events.PolarisEventListener;
import org.apache.polaris.service.events.TestPolarisEventListener;
import org.apache.polaris.service.exception.FakeAzureHttpResponse;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.apache.polaris.service.quarkus.config.QuarkusReservedProperties;
import org.apache.polaris.service.quarkus.test.TestData;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TableCleanupTaskHandler;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.AbstractCollectionAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.configuration.PreferredAssumptionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@TestProfile(IcebergCatalogTest.Profile.class)
public abstract class IcebergCatalogTest extends CatalogTests<IcebergCatalog> {
  static {
    org.assertj.core.api.Assumptions.setPreferredAssumptionException(
        PreferredAssumptionException.JUNIT5);
  }

  DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.features.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"",
          "true",
          "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"",
          "true",
          "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\",\"S3\"]",
          "polaris.features.\"LIST_PAGINATION_ENABLED\"",
          "true",
          "polaris.event-listener.type",
          "test",
          "polaris.readiness.ignore-severe-issues",
          "true",
          "polaris.features.\"ALLOW_TABLE_LOCATION_OVERLAP\"",
          "true");
    }
  }

  private static final String VIEW_QUERY = "select * from ns1.layer1_table";

  public static final String CATALOG_NAME = "polaris-catalog";
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  public static Map<String, String> TABLE_PREFIXES =
      Map.of(
          CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
          "catalog-default-key1",
          CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
          "catalog-default-key2",
          CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
          "catalog-default-key3",
          CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
          "catalog-override-key3",
          CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
          "catalog-override-key4");

  @Inject MetaStoreManagerFactory managerFactory;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject UserSecretsManagerFactory userSecretsManagerFactory;
  @Inject PolarisDiagnostics diagServices;
  @Inject PolarisEventListener polarisEventListener;

  private IcebergCatalog catalog;
  private String realmName;
  private PolarisMetaStoreManager metaStoreManager;
  private UserSecretsManager userSecretsManager;
  private PolarisCallContext polarisContext;
  private PolarisAdminService adminService;
  private PolarisEntityManager entityManager;
  private FileIOFactory fileIOFactory;
  private InMemoryFileIO fileIO;
  private PolarisEntity catalogEntity;
  private SecurityContext securityContext;
  private TestPolarisEventListener testPolarisEventListener;
  private ReservedProperties reservedProperties;

  protected String getRealmName() {
    return realmName;
  }

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @Nullable
  protected abstract InMemoryEntityCache createEntityCache(
      PolarisMetaStoreManager metaStoreManager);

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    metaStoreManager = managerFactory.getOrCreateMetaStoreManager(realmContext);
    userSecretsManager = userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);
    polarisContext =
        new PolarisCallContext(
            realmContext,
            managerFactory.getOrCreateSessionSupplier(realmContext).get(),
            diagServices,
            configurationStore,
            Clock.systemDefaultZone());

    entityManager =
        new PolarisEntityManager(
            metaStoreManager,
            new StorageCredentialCache(realmContext, configurationStore),
            createEntityCache(metaStoreManager));

    PrincipalEntity rootEntity =
        new PrincipalEntity(
            PolarisEntity.of(
                metaStoreManager
                    .readEntityByName(
                        polarisContext,
                        null,
                        PolarisEntityType.PRINCIPAL,
                        PolarisEntitySubType.NULL_SUBTYPE,
                        "root")
                    .getEntity()));

    AuthenticatedPolarisPrincipal authenticatedRoot =
        new AuthenticatedPolarisPrincipal(rootEntity, Set.of());

    securityContext = Mockito.mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(authenticatedRoot);
    when(securityContext.isUserInRole(isA(String.class))).thenReturn(true);

    reservedProperties = new QuarkusReservedProperties() {};

    adminService =
        new PolarisAdminService(
            polarisContext,
            entityManager,
            metaStoreManager,
            userSecretsManager,
            securityContext,
            new PolarisAuthorizerImpl(new PolarisConfigurationStore() {}),
            reservedProperties);

    String storageLocation = "s3://my-bucket/path/to/data";
    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();

    catalogEntity =
        adminService.createCatalog(
            new CreateCatalogRequest(
                toCatalog(
                    new CatalogEntity.Builder()
                        .setName(CATALOG_NAME)
                        .setDefaultBaseLocation(storageLocation)
                        .setReplaceNewLocationPrefixWithCatalogDefault("file:")
                        .addProperty(
                            FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(),
                            "true")
                        .addProperty(
                            FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                            "true")
                        .addProperty(
                            FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true")
                        .setStorageConfigurationInfo(
                            polarisContext, storageConfigModel, storageLocation)
                        .build())));

    RealmEntityManagerFactory realmEntityManagerFactory =
        new RealmEntityManagerFactory(createMockMetaStoreManagerFactory());
    this.fileIOFactory =
        new DefaultFileIOFactory(realmEntityManagerFactory, managerFactory, configurationStore);

    StsClient stsClient = Mockito.mock(StsClient.class);
    when(stsClient.assumeRole(isA(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(TEST_ACCESS_KEY)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build())
                .build());
    PolarisStorageIntegration<AwsStorageConfigurationInfo> storageIntegration =
        new AwsCredentialsStorageIntegration(stsClient);
    when(storageIntegrationProvider.getStorageIntegrationForConfig(
            isA(AwsStorageConfigurationInfo.class)))
        .thenReturn((PolarisStorageIntegration) storageIntegration);

    this.catalog = initCatalog("my-catalog", ImmutableMap.of());
    testPolarisEventListener = (TestPolarisEventListener) polarisEventListener;
  }

  @AfterEach
  public void after() throws IOException {
    catalog().close();
    metaStoreManager.purge(polarisContext);
  }

  @Override
  protected IcebergCatalog catalog() {
    return catalog;
  }

  /**
   * Initialize a IcebergCatalog for testing.
   *
   * @param catalogName this parameter is currently unused.
   * @param additionalProperties additional properties to apply on top of the default test settings
   * @return a configured instance of IcebergCatalog
   */
  @Override
  protected IcebergCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, CATALOG_NAME);
    TaskExecutor taskExecutor = Mockito.mock();
    IcebergCatalog icebergCatalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory,
            polarisEventListener);
    fileIO = new InMemoryFileIO();
    icebergCatalog.setCatalogFileIo(fileIO);
    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putAll(TABLE_PREFIXES)
            .putAll(additionalProperties);
    icebergCatalog.initialize(CATALOG_NAME, propertiesBuilder.buildKeepingLast());
    return icebergCatalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return true;
  }

  protected boolean supportsNotifications() {
    return true;
  }

  private MetaStoreManagerFactory createMockMetaStoreManagerFactory() {
    return new MetaStoreManagerFactory() {
      @Override
      public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
        return metaStoreManager;
      }

      @Override
      public Supplier<TransactionalPersistence> getOrCreateSessionSupplier(
          RealmContext realmContext) {
        return () -> ((TransactionalPersistence) polarisContext.getMetaStore());
      }

      @Override
      public StorageCredentialCache getOrCreateStorageCredentialCache(RealmContext realmContext) {
        return new StorageCredentialCache(realmContext, configurationStore);
      }

      @Override
      public InMemoryEntityCache getOrCreateEntityCache(RealmContext realmContext) {
        return new InMemoryEntityCache(realmContext, configurationStore, metaStoreManager);
      }

      @Override
      public Map<String, PrincipalSecretsResult> bootstrapRealms(
          Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
        throw new NotImplementedException("Bootstrapping realms is not supported");
      }

      @Override
      public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
        throw new NotImplementedException("Purging realms is not supported");
      }
    };
  }

  @Test
  public void testEmptyNamespace() {
    IcebergCatalog catalog = catalog();
    TableIdentifier tableInRootNs = TableIdentifier.of("table");
    String expectedMessage = "Namespace does not exist: ''";

    ThrowingCallable createEmptyNamespace = () -> catalog.createNamespace(Namespace.empty());
    Assertions.assertThatThrownBy(createEmptyNamespace)
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Cannot create root namespace, as it already exists implicitly.");

    ThrowingCallable dropEmptyNamespace = () -> catalog.dropNamespace(Namespace.empty());
    Assertions.assertThatThrownBy(dropEmptyNamespace)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot drop root namespace");

    ThrowingCallable createTable = () -> catalog.createTable(tableInRootNs, SCHEMA);
    Assertions.assertThatThrownBy(createTable)
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining(expectedMessage);

    ThrowingCallable createView =
        () ->
            catalog
                .buildView(tableInRootNs)
                .withSchema(SCHEMA)
                .withDefaultNamespace(Namespace.empty())
                .withQuery("spark", VIEW_QUERY)
                .create();
    Assertions.assertThatThrownBy(createView)
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining(expectedMessage);

    ThrowingCallable listTables = () -> catalog.listTables(Namespace.empty());
    Assertions.assertThatThrownBy(listTables)
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining(expectedMessage);

    ThrowingCallable listViews = () -> catalog.listViews(Namespace.empty());
    Assertions.assertThatThrownBy(listViews)
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining(expectedMessage);
  }

  @Test
  public void testRenameTableMissingDestinationNamespace() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");

    IcebergCatalog catalog = catalog();
    catalog.createNamespace(NS);

    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Source table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThat(catalog.tableExists(TABLE))
        .as("Table should exist after create")
        .isTrue();

    Namespace newNamespace = Namespace.of("nonexistent_namespace");
    TableIdentifier renamedTable = TableIdentifier.of(newNamespace, "table_renamed");

    Assertions.assertThat(catalog.namespaceExists(newNamespace))
        .as("Destination namespace should not exist before rename")
        .isFalse();

    Assertions.assertThat(catalog.tableExists(renamedTable))
        .as("Destination table should not exist before rename")
        .isFalse();

    Assertions.assertThatThrownBy(() -> catalog.renameTable(TABLE, renamedTable))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist");

    Assertions.assertThat(catalog.namespaceExists(newNamespace))
        .as("Destination namespace should not exist after failed rename")
        .isFalse();

    Assertions.assertThat(catalog.tableExists(renamedTable))
        .as("Table should not exist after failed rename")
        .isFalse();
  }

  @Test
  public void testCreateNestedNamespaceUnderMissingParent() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supoprted");

    IcebergCatalog catalog = catalog();

    Namespace child1 = Namespace.of("parent", "child1");

    Assertions.assertThatThrownBy(() -> catalog.createNamespace(child1))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Parent");
  }

  @Test
  public void testConcurrentWritesWithRollbackNonEmptyTable() {
    IcebergCatalog catalog = this.catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B
    // set the snapshot property in the summary to make this snapshot
    // rollback-able.
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    try {
      // Now call IRC server to commit delete operation.
      CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
      catalogHandlerUtils.commit(((BaseTable) catalog.loadTable(TABLE)).operations(), request);
    } catch (Exception e) {
      fail("Rollback Compaction on conflict feature failed : " + e);
    }

    table.refresh();

    // Assert only 2 snapshots and no snapshot of REPLACE left.
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      // no snapshot in the hierarchy for REPLACE operations
      assertThat(currentSnapshot.operation()).isNotEqualTo(DataOperations.REPLACE);
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(2);

    // Inspect the files 1 DELETE file i.e. FILE_A_DELETES and 1 DATA FILE FILE_A
    try {
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        List<CharSequence> dataFilePaths =
            Streams.stream(tasks)
                .map(ContentScanTask::file)
                .map(ContentFile::location)
                .collect(Collectors.toList());
        List<CharSequence> deleteFilePaths =
            Streams.stream(tasks)
                .flatMap(t -> t.deletes().stream().map(ContentFile::location))
                .collect(Collectors.toList());
        ((ListAssert)
                Assertions.assertThat(dataFilePaths)
                    .as("Should contain expected number of data files", new Object[0]))
            .hasSize(1);
        ((ListAssert)
                Assertions.assertThat(deleteFilePaths)
                    .as("Should contain expected number of delete files", new Object[0]))
            .hasSize(1);
        ((AbstractCollectionAssert)
                Assertions.assertThat(CharSequenceSet.of(dataFilePaths))
                    .as("Should contain correct file paths", new Object[0]))
            .isEqualTo(
                CharSequenceSet.of(
                    Iterables.transform(Arrays.asList(FILE_A), ContentFile::location)));
        ((AbstractCollectionAssert)
                Assertions.assertThat(CharSequenceSet.of(deleteFilePaths))
                    .as("Should contain correct file paths", new Object[0]))
            .isEqualTo(
                CharSequenceSet.of(
                    Iterables.transform(Arrays.asList(FILE_A_DELETES), ContentFile::location)));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Test
  public void testConcurrentWritesWithRollbackWithNonReplaceSnapshotInBetween() {
    IcebergCatalog catalog = this.catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B
    // commit the transaction.
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    // commit FILE_C
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).commit();
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
    Assertions.assertThatThrownBy(
            () ->
                catalogHandlerUtils.commit(
                    ((BaseTable) catalog.loadTable(TABLE)).operations(), request))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: branch main has changed");

    table.refresh();

    // Assert only 3 snapshots
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(3);
    this.assertFiles(catalog.loadTable(TABLE), FILE_B, FILE_C);
  }

  @Test
  public void
      testConcurrentWritesWithRollbackEnableWithToRollbackSnapshotReferencedByOtherBranch() {
    IcebergCatalog catalog = this.catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    Table t = catalog.loadTable(TABLE);
    // add another branch B
    t.manageSnapshots()
        .createBranch("non-main")
        .setCurrentSnapshot(t.currentSnapshot().snapshotId())
        .commit();
    // now add more files to non-main branch
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).toBranch("non-main").commit();
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
    Assertions.assertThatThrownBy(
            () ->
                catalogHandlerUtils.commit(
                    ((BaseTable) catalog.loadTable(TABLE)).operations(), request))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: branch main has changed");

    table.refresh();

    // Assert only 3 snapshots
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(2);
    this.assertFiles(catalog.loadTable(TABLE), FILE_B);
  }

  @Test
  public void testConcurrentWritesWithRollbackWithConcurrentWritesToDifferentBranches() {
    IcebergCatalog catalog = this.catalog();
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table = catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).create();
    this.assertNoFiles(table);

    // commit FILE_A to main branch
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_A).commit();
    this.assertFiles(catalog.loadTable(TABLE), FILE_A);
    table.refresh();

    Table t = catalog.loadTable(TABLE);
    // add another branch B
    t.manageSnapshots()
        .createBranch("non-main")
        .setCurrentSnapshot(t.currentSnapshot().snapshotId())
        .commit();

    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // Apply the deletes based on FILE_A
    // this should conflict when we try to commit without the change.
    RowDelta originalRowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(lastSnapshotId)
            .validateDataFilesExist(List.of(FILE_A.location()));
    // Make client ready with updates, don't reach out to IRC server yet
    Snapshot s = originalRowDelta.apply();
    TableOperations ops = ((BaseTable) catalog.loadTable(TABLE)).operations();
    TableMetadata base = ops.current();
    TableMetadata.Builder update = TableMetadata.buildFrom(base);
    update.setBranchSnapshot(s, "main");
    TableMetadata updatedMetadata = update.build();
    List<MetadataUpdate> updates = updatedMetadata.changes();
    List<UpdateRequirement> requirements = UpdateRequirements.forUpdateTable(base, updates);
    UpdateTableRequest request = UpdateTableRequest.create(TABLE, requirements, updates);

    // replace FILE_A with FILE_B on main branch
    catalog
        .loadTable(TABLE)
        .newRewrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .set("polaris.internal.conflict-resolution.by-operation-type.replace", "rollback")
        .commit();

    // now add more files to non-main branch, this will make sequence number non monotonic for main
    // branch
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).toBranch("non-main").commit();
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(5, true);
    Assertions.assertThatThrownBy(
            () ->
                catalogHandlerUtils.commit(
                    ((BaseTable) catalog.loadTable(TABLE)).operations(), request))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Requirement failed: branch main has changed");

    table.refresh();

    // Assert only 3 snapshots
    Snapshot currentSnapshot = table.snapshot(table.refs().get("main").snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }
    assertThat(totalSnapshots).isEqualTo(2);
    this.assertFiles(catalog.loadTable(TABLE), FILE_B);
  }

  @Test
  public void testValidateNotificationWhenTableAndNamespacesDontExist() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/validate_table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    // For a VALIDATE request we can pass in a full metadata JSON filename or just the table's
    // metadata directory; either way the path will be validated to be under the allowed locations,
    // but any actual metadata JSON file will not be accessed.
    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.VALIDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    // We should be able to send the notification without creating the metadata file since it's
    // only validating the ability to send the CREATE/UPDATE notification possibly before actually
    // creating the table at all on the remote catalog.
    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should not be created")
        .isFalse();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should not be created for a VALIDATE notification")
        .isFalse();

    // Now also check that despite creating the metadata file, the validation call still doesn't
    // create any namespaces or tables.
    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should not be created")
        .isFalse();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should not be created for a VALIDATE notification")
        .isFalse();
  }

  @Test
  public void testValidateNotificationInDisallowedLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified in the create will be forbidden.
    // For a VALIDATE call we can pass in the metadata/ prefix itself instead of a metadata JSON
    // filename.
    final String tableLocation = "s3://forbidden-table-location/table/";
    final String tableMetadataLocation = tableLocation + "metadata/";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.VALIDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid location");
  }

  @Test
  public void testValidateNotificationFailToCreateFileIO() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified in the create will be allowed, but
    // we'll inject a separate ForbiddenException during FileIO instantiation.
    // For a VALIDATE call we can pass in the metadata/ prefix itself instead of a metadata JSON
    // filename.
    final String tableLocation = "s3://externally-owned-bucket/validate_table/";
    final String tableMetadataLocation = tableLocation + "metadata/";
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, catalog().name());
    FileIOFactory fileIOFactory =
        spy(
            new DefaultFileIOFactory(
                new RealmEntityManagerFactory(createMockMetaStoreManagerFactory()),
                managerFactory,
                configurationStore));
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            Mockito.mock(TaskExecutor.class),
            fileIOFactory,
            polarisEventListener);
    catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.VALIDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    doThrow(new ForbiddenException("Fake failure applying downscoped credentials"))
        .when(fileIOFactory)
        .loadFileIO(any(), any(), any(), any(), any(), any(), any());
    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Fake failure applying downscoped credentials");
  }

  @Test
  public void testUpdateNotificationWhenTableAndNamespacesDontExist() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should be created")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should be created on receiving notification")
        .isTrue();
  }

  @Test
  public void testUpdateNotificationCreateTableInDisallowedLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified in the create will be forbidden.
    final String tableLocation = "s3://forbidden-table-location/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid location");
  }

  @Test
  public void testCreateNotificationCreateTableInExternalLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified is outside of the table's base location
    // according to the
    // metadata. We assume this is fraudulent and disallowed
    final String tableSuffix = UUID.randomUUID().toString();
    final String tableLocation =
        String.format("s3://my-bucket/path/to/data/my_table_%s/", tableSuffix);
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    final String anotherTableLocation =
        String.format("s3://my-bucket/path/to/data/another_table_%s/", tableSuffix);

    metaStoreManager.updateEntityPropertiesIfNotChanged(
        polarisContext,
        List.of(PolarisEntity.toCore(catalogEntity)),
        new CatalogEntity.Builder(CatalogEntity.of(catalogEntity))
            .addProperty(
                FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "false")
            .addProperty(
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .build());
    IcebergCatalog catalog = catalog();
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID()
            .setLocation(anotherTableLocation)
            .addSchema(SCHEMA)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();
    TableMetadataParser.write(tableMetadata, fileIO.newOutputFile(tableMetadataLocation));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "my_table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.CREATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("is not allowed outside of table location");
  }

  @Test
  public void testCreateNotificationCreateTableOutsideOfMetadataLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableSuffix = UUID.randomUUID().toString();
    // The location of the metadata JSON file specified is outside of the table's metadata directory
    // according to the
    // metadata. We assume this is fraudulent and disallowed
    final String tableLocation =
        String.format("s3://my-bucket/path/to/data/my_table_%s/", tableSuffix);
    final String tableMetadataLocation = tableLocation + "metadata/v3.metadata.json";

    // this passes the first validation, since it's within the namespace subdirectory, but
    // the location is in another table's subdirectory
    final String anotherTableLocation =
        String.format("s3://my-bucket/path/to/data/another_table_%s", tableSuffix);

    metaStoreManager.updateEntityPropertiesIfNotChanged(
        polarisContext,
        List.of(PolarisEntity.toCore(catalogEntity)),
        new CatalogEntity.Builder(CatalogEntity.of(catalogEntity))
            .addProperty(
                FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "false")
            .addProperty(
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .build());
    IcebergCatalog catalog = catalog();
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID()
            .setLocation(anotherTableLocation)
            .addSchema(SCHEMA)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();
    TableMetadataParser.write(tableMetadata, fileIO.newOutputFile(tableMetadataLocation));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "my_table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.CREATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("is not allowed outside of table location");
  }

  @Test
  public void testUpdateNotificationCreateTableInExternalLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified is outside of the table's base location
    // according to the
    // metadata. We assume this is fraudulent and disallowed
    final String tableSuffix = UUID.randomUUID().toString();
    final String tableLocation =
        String.format("s3://my-bucket/path/to/data/my_table_%s/", tableSuffix);
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    final String anotherTableLocation =
        String.format("s3://my-bucket/path/to/data/another_table_%s/", tableSuffix);

    metaStoreManager.updateEntityPropertiesIfNotChanged(
        polarisContext,
        List.of(PolarisEntity.toCore(catalogEntity)),
        new CatalogEntity.Builder(CatalogEntity.of(catalogEntity))
            .addProperty(
                FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "false")
            .addProperty(
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .build());
    IcebergCatalog catalog = catalog();

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "my_table");

    NotificationRequest createRequest = new NotificationRequest();
    createRequest.setNotificationType(NotificationType.CREATE);
    TableUpdateNotification create = new TableUpdateNotification();
    create.setMetadataLocation(tableMetadataLocation);
    create.setTableName(table.name());
    create.setTableUuid(UUID.randomUUID().toString());
    create.setTimestamp(230950845L);
    createRequest.setPayload(create);

    // the create should succeed
    catalog.sendNotification(table, createRequest);

    // now craft the malicious metadata file
    final String maliciousMetadataFile = tableLocation + "metadata/v2.metadata.json";
    TableMetadata tableMetadata =
        TableMetadata.buildFromEmpty()
            .assignUUID()
            .setLocation(anotherTableLocation)
            .addSchema(SCHEMA)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .build();
    TableMetadataParser.write(tableMetadata, fileIO.newOutputFile(maliciousMetadataFile));

    NotificationRequest updateRequest = new NotificationRequest();
    updateRequest.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(maliciousMetadataFile);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950849L);
    updateRequest.setPayload(update);

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, updateRequest))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("is not allowed outside of table location");
  }

  @Test
  public void testUpdateNotificationCreateTableWithLocalFilePrefix() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified in the create will be forbidden.
    final String metadataLocation = "file:///etc/metadata.json/../passwd";
    String catalogWithoutStorage = "catalogWithoutStorage";

    PolarisEntity catalogEntity =
        adminService.createCatalog(
            new CreateCatalogRequest(
                toCatalog(
                    new CatalogEntity.Builder()
                        .setDefaultBaseLocation("file://")
                        .setName(catalogWithoutStorage)
                        .build())));

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, catalogWithoutStorage);
    TaskExecutor taskExecutor = Mockito.mock();
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory,
            polarisEventListener);
    catalog.initialize(
        catalogWithoutStorage,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(metadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        metadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(metadataLocation)).getBytes(UTF_8));

    if (!polarisContext
        .getConfigurationStore()
        .getConfiguration(
            polarisContext.getRealmContext(), FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES)
        .contains("FILE")) {
      Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Invalid location");
    }
  }

  @Test
  public void testUpdateNotificationCreateTableWithHttpPrefix() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    String catalogName = "catalogForMaliciousDomain";

    adminService.createCatalog(
        new CreateCatalogRequest(
            toCatalog(
                new CatalogEntity.Builder()
                    .setDefaultBaseLocation("http://maliciousdomain.com")
                    .setName(catalogName)
                    .build())));

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, catalogName);
    TaskExecutor taskExecutor = Mockito.mock();
    InMemoryFileIO localFileIO = new InMemoryFileIO();
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory,
            polarisEventListener);
    catalog.initialize(
        catalogName,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    // The location of the metadata JSON file specified in the create will be forbidden.
    final String metadataLocation = "http://maliciousdomain.com/metadata.json";
    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(metadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        metadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(metadataLocation)).getBytes(UTF_8));

    if (!polarisContext
        .getConfigurationStore()
        .getConfiguration(
            polarisContext.getRealmContext(), FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES)
        .contains("FILE")) {
      Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Invalid location");
    }

    // It also fails if we try to use https
    final String httpsMetadataLocation = "https://maliciousdomain.com/metadata.json";
    NotificationRequest newRequest = new NotificationRequest();
    newRequest.setNotificationType(NotificationType.UPDATE);
    newRequest.setPayload(
        new TableUpdateNotification(
            table.name(), 230950845L, UUID.randomUUID().toString(), httpsMetadataLocation, null));

    fileIO.addFile(
        httpsMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(metadataLocation)).getBytes(UTF_8));

    if (!polarisContext
        .getConfigurationStore()
        .getConfiguration(
            polarisContext.getRealmContext(), FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES)
        .contains("FILE")) {
      Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, newRequest))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("Invalid location");
    }
  }

  @Test
  public void testUpdateNotificationWhenNamespacesExist() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should be created")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should be created on receiving notification")
        .isTrue();
  }

  @Test
  public void testUpdateNotificationWhenTableExists() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    TableIdentifier table = TableIdentifier.of(namespace, "table");

    catalog.createTable(
        table,
        new Schema(
            Types.NestedField.required(1, "intType", Types.IntegerType.get()),
            Types.NestedField.required(2, "stringType", Types.StringType.get())));

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should be created")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should be created on receiving notification")
        .isTrue();
  }

  @Test
  public void testUpdateNotificationWhenTableExistsInDisallowedLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    // The location of the metadata JSON file specified in the update will be forbidden.
    final String tableLocation = "s3://forbidden-table-location/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    TableIdentifier table = TableIdentifier.of(namespace, "table");

    catalog.createTable(
        table,
        new Schema(
            Types.NestedField.required(1, "intType", Types.IntegerType.get()),
            Types.NestedField.required(2, "stringType", Types.StringType.get())));

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid location");
  }

  @Test
  public void testUpdateNotificationRejectOutOfOrderTimestamp() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    long timestamp = 230950845L;
    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.CREATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(timestamp);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    catalog.sendNotification(table, request);

    // Send a notification with a timestamp same as that of the previous notification, should fail
    NotificationRequest request2 = new NotificationRequest();
    request2.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update2 = new TableUpdateNotification();
    update2.setMetadataLocation(tableLocation + "metadata/v2.metadata.json");
    update2.setTableName(table.name());
    update2.setTableUuid(UUID.randomUUID().toString());
    update2.setTimestamp(timestamp);
    request2.setPayload(update2);

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request2))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining(
            "A notification with a newer timestamp has been processed for table parent.child1.table");

    // Verify that DROP notification won't be rejected due to timestamp
    NotificationRequest request3 = new NotificationRequest();
    request3.setNotificationType(NotificationType.DROP);
    TableUpdateNotification update3 = new TableUpdateNotification();
    update3.setMetadataLocation(tableLocation + "metadata/v2.metadata.json");
    update3.setTableName(table.name());
    update3.setTableUuid(UUID.randomUUID().toString());
    update3.setTimestamp(timestamp);
    request3.setPayload(update3);

    Assertions.assertThat(catalog.sendNotification(table, request3))
        .as("Drop notification should not fail despite timestamp being outdated")
        .isTrue();
  }

  @Test
  public void testUpdateNotificationWhenTableExistsFileSpecifiesDisallowedLocation() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    TableIdentifier table = TableIdentifier.of(namespace, "table");

    catalog.createTable(
        table,
        new Schema(
            Types.NestedField.required(1, "intType", Types.IntegerType.get()),
            Types.NestedField.required(2, "stringType", Types.StringType.get())));

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.UPDATE);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    // Though the metadata JSON file itself is in an allowed location, make it internally specify
    // a forbidden table location.
    TableMetadata forbiddenMetadata =
        createSampleTableMetadata("s3://forbidden-table-location/table/");
    fileIO.addFile(
        tableMetadataLocation, TableMetadataParser.toJson(forbiddenMetadata).getBytes(UTF_8));

    Assertions.assertThatThrownBy(() -> catalog.sendNotification(table, request))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid location");
  }

  @Test
  public void testDropNotificationWhenTableAndNamespacesDontExist() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.DROP);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should fail since the target table doesn't exist")
        .isFalse();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should not be created")
        .isFalse();
    Assertions.assertThat(catalog.tableExists(table)).as("Table should not exist").isFalse();
  }

  @Test
  public void testDropNotificationWhenNamespacesExist() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    TableIdentifier table = TableIdentifier.of(namespace, "table");

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.DROP);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should fail since table doesn't exist")
        .isFalse();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should exist")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should not be created on receiving notification")
        .isFalse();
  }

  @Test
  public void testDropNotificationWhenTableExists() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");
    Assumptions.assumeTrue(
        supportsNotifications(), "Only applicable if notifications are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    IcebergCatalog catalog = catalog();

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    TableIdentifier table = TableIdentifier.of(namespace, "table");

    catalog.createTable(
        table,
        new Schema(
            Types.NestedField.required(1, "intType", Types.IntegerType.get()),
            Types.NestedField.required(2, "stringType", Types.StringType.get())));

    NotificationRequest request = new NotificationRequest();
    request.setNotificationType(NotificationType.DROP);
    TableUpdateNotification update = new TableUpdateNotification();
    update.setMetadataLocation(tableMetadataLocation);
    update.setTableName(table.name());
    update.setTableUuid(UUID.randomUUID().toString());
    update.setTimestamp(230950845L);
    request.setPayload(update);

    fileIO.addFile(
        tableMetadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(tableLocation)).getBytes(UTF_8));

    Assertions.assertThat(catalog.sendNotification(table, request))
        .as("Notification should be sent successfully")
        .isTrue();
    Assertions.assertThat(catalog.namespaceExists(namespace))
        .as("Intermediate namespaces should already exist")
        .isTrue();
    Assertions.assertThat(catalog.tableExists(table))
        .as("Table should be dropped on receiving notification")
        .isFalse();
  }

  @Test
  @Override
  public void testDropTableWithPurge() {
    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Assertions.assertThatPredicate(catalog::tableExists)
        .as("Table should not exist before create")
        .rejects(TABLE);

    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThatPredicate(catalog::tableExists)
        .as("Table should exist after create")
        .accepts(TABLE);
    Assertions.assertThat(table).isInstanceOf(BaseTable.class);
    TableMetadata tableMetadata = ((BaseTable) table).operations().current();

    boolean dropped = catalog.dropTable(TABLE, true);
    Assertions.assertThat(dropped)
        .as("Should drop a table that does exist", new Object[0])
        .isTrue();
    Assertions.assertThatPredicate(catalog::tableExists)
        .as("Table should not exist after drop")
        .rejects(TABLE);
    List<PolarisBaseEntity> tasks =
        metaStoreManager
            .loadTasks(polarisContext, "testExecutor", PageToken.fromLimit(1))
            .getEntities();
    Assertions.assertThat(tasks).hasSize(1);
    TaskEntity taskEntity = TaskEntity.of(tasks.get(0));
    EnumMap<StorageAccessProperty, String> credentials =
        metaStoreManager
            .getSubscopedCredsForEntity(
                polarisContext,
                0,
                taskEntity.getId(),
                taskEntity.getType(),
                true,
                Set.of(tableMetadata.location()),
                Set.of(tableMetadata.location()))
            .getCredentials();
    Assertions.assertThat(credentials)
        .isNotNull()
        .isNotEmpty()
        .containsEntry(StorageAccessProperty.AWS_KEY_ID, TEST_ACCESS_KEY)
        .containsEntry(StorageAccessProperty.AWS_SECRET_KEY, SECRET_ACCESS_KEY)
        .containsEntry(StorageAccessProperty.AWS_TOKEN, SESSION_TOKEN);
    MetaStoreManagerFactory metaStoreManagerFactory = createMockMetaStoreManagerFactory();
    FileIO fileIO =
        new TaskFileIOSupplier(
                new DefaultFileIOFactory(
                    new RealmEntityManagerFactory(metaStoreManagerFactory),
                    metaStoreManagerFactory,
                    configurationStore))
            .apply(taskEntity, polarisContext);
    Assertions.assertThat(fileIO).isNotNull().isInstanceOf(ExceptionMappingFileIO.class);
    Assertions.assertThat(((ExceptionMappingFileIO) fileIO).getInnerIo())
        .isInstanceOf(InMemoryFileIO.class);
  }

  @Test
  public void testDropTableWithPurgeDisabled() {
    // Create a catalog with purge disabled:
    String noPurgeCatalogName = CATALOG_NAME + "_no_purge";
    String storageLocation = "s3://testDropTableWithPurgeDisabled/data";
    AwsStorageConfigInfo noPurgeStorageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .build();
    adminService.createCatalog(
        new CreateCatalogRequest(
            toCatalog(
                new CatalogEntity.Builder()
                    .setName(noPurgeCatalogName)
                    .setDefaultBaseLocation(storageLocation)
                    .setReplaceNewLocationPrefixWithCatalogDefault("file:")
                    .addProperty(
                        FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                    .addProperty(
                        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                        "true")
                    .addProperty(
                        FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "false")
                    .setStorageConfigurationInfo(
                        polarisContext, noPurgeStorageConfigModel, storageLocation)
                    .build())));
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, noPurgeCatalogName);
    IcebergCatalog noPurgeCatalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            Mockito.mock(),
            fileIOFactory,
            polarisEventListener);
    noPurgeCatalog.initialize(
        noPurgeCatalogName,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    if (this.requiresNamespaceCreate()) {
      noPurgeCatalog.createNamespace(NS);
    }

    Assertions.assertThatPredicate(noPurgeCatalog::tableExists)
        .as("Table should not exist before create")
        .rejects(TABLE);

    Table table = noPurgeCatalog.buildTable(TABLE, SCHEMA).create();
    Assertions.assertThatPredicate(noPurgeCatalog::tableExists)
        .as("Table should exist after create")
        .accepts(TABLE);
    Assertions.assertThat(table).isInstanceOf(BaseTable.class);

    // Attempt to drop the table:
    Assertions.assertThatThrownBy(() -> noPurgeCatalog.dropTable(TABLE, true))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.key);
  }

  private TableMetadata createSampleTableMetadata(String tableLocation) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "intType", Types.IntegerType.get()),
            Types.NestedField.required(2, "stringType", Types.StringType.get()));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("intType").withSpecId(1000).build();

    return TableMetadata.newTableMetadata(schema, partitionSpec, tableLocation, ImmutableMap.of());
  }

  private void createNonExistingNamespaces(Namespace namespace) {
    // Pre-create namespaces if they don't exist
    for (int i = 1; i <= namespace.length(); i++) {
      Namespace nsLevel = Namespace.of(Arrays.copyOf(namespace.levels(), i));
      if (!catalog.namespaceExists(nsLevel)) {
        catalog.createNamespace(nsLevel);
      }
    }
  }

  @ParameterizedTest
  @MethodSource
  public void testRetriableException(Exception exception, boolean shouldRetry) {
    Assertions.assertThat(IcebergCatalog.SHOULD_RETRY_REFRESH_PREDICATE.test(exception))
        .isEqualTo(shouldRetry);
  }

  static Stream<Arguments> testRetriableException() {
    Set<Integer> NON_RETRYABLE_CODES = Set.of(401, 403, 404);
    Set<Integer> RETRYABLE_CODES = Set.of(408, 504);

    // Create a map of HTTP code returned from a cloud provider to whether it should be retried
    Map<Integer, Boolean> cloudCodeMappings = new HashMap<>();
    NON_RETRYABLE_CODES.forEach(code -> cloudCodeMappings.put(code, false));
    RETRYABLE_CODES.forEach(code -> cloudCodeMappings.put(code, true));

    return Stream.of(
            Stream.of(
                Arguments.of(new RuntimeException(new IOException("Connection reset")), true),
                Arguments.of(RetryableException.builder().build(), true),
                Arguments.of(NonRetryableException.builder().build(), false)),
            IcebergExceptionMapper.getAccessDeniedHints().stream()
                .map(hint -> Arguments.of(new RuntimeException(hint), false)),
            cloudCodeMappings.entrySet().stream()
                .flatMap(
                    entry ->
                        Stream.of(
                            Arguments.of(
                                new HttpResponseException(
                                    "", new FakeAzureHttpResponse(entry.getKey()), ""),
                                entry.getValue()),
                            Arguments.of(
                                new StorageException(entry.getKey(), ""), entry.getValue()))),
            IcebergExceptionMapper.RETRYABLE_AZURE_HTTP_CODES.stream()
                .map(
                    code ->
                        Arguments.of(
                            new HttpResponseException("", new FakeAzureHttpResponse(code), ""),
                            true)))
        .flatMap(Function.identity());
  }

  @Test
  public void testFileIOWrapper() {
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, CATALOG_NAME);

    MeasuredFileIOFactory measured =
        new MeasuredFileIOFactory(
            new RealmEntityManagerFactory(createMockMetaStoreManagerFactory()),
            managerFactory,
            configurationStore);
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            Mockito.mock(),
            measured,
            polarisEventListener);
    catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Assertions.assertThat(measured.getNumOutputFiles() + measured.getInputBytes())
        .as("Nothing was created yet")
        .isEqualTo(0);

    catalog.createNamespace(TestData.NAMESPACE);
    Table table = catalog.buildTable(TestData.TABLE, TestData.SCHEMA).create();

    // Asserting greaterThan 0 is sufficient for validating that the wrapper works without making
    // assumptions about the
    // specific implementations of table operations.
    Assertions.assertThat(measured.getNumOutputFiles()).as("A table was created").isGreaterThan(0);

    table.updateProperties().set("foo", "bar").commit();
    Assertions.assertThat(measured.getInputBytes())
        .as("A table was read and written, but a trip to storage was made")
        .isEqualTo(0);

    Assertions.assertThat(catalog.dropTable(TestData.TABLE))
        .as("Table deletion should succeed")
        .isTrue();
    TaskEntity taskEntity =
        TaskEntity.of(
            metaStoreManager
                .loadTasks(polarisContext, "testExecutor", PageToken.fromLimit(1))
                .getEntities()
                .getFirst());
    Map<String, String> properties = taskEntity.getInternalPropertiesAsMap();
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO");
    taskEntity =
        TaskEntity.of(
            new PolarisBaseEntity.Builder(taskEntity).internalPropertiesAsMap(properties).build());
    TaskFileIOSupplier taskFileIOSupplier =
        new TaskFileIOSupplier(
            new FileIOFactory() {
              @Override
              public FileIO loadFileIO(
                  @Nonnull CallContext callContext,
                  @Nonnull String ioImplClassName,
                  @Nonnull Map<String, String> properties,
                  @Nonnull TableIdentifier identifier,
                  @Nonnull Set<String> tableLocations,
                  @Nonnull Set<PolarisStorageActions> storageActions,
                  @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
                return measured.loadFileIO(
                    callContext,
                    "org.apache.iceberg.inmemory.InMemoryFileIO",
                    Map.of(),
                    TABLE,
                    Set.of(table.location()),
                    Set.of(PolarisStorageActions.ALL),
                    Mockito.mock());
              }
            });

    TableCleanupTaskHandler handler =
        new TableCleanupTaskHandler(
            Mockito.mock(), createMockMetaStoreManagerFactory(), taskFileIOSupplier);
    handler.handleTask(taskEntity, polarisContext);
    Assertions.assertThat(measured.getNumDeletedFiles()).as("A table was deleted").isGreaterThan(0);
  }

  @Test
  public void testRegisterTableWithSlashlessMetadataLocation() {
    IcebergCatalog catalog = catalog();
    Assertions.assertThatThrownBy(
            () -> catalog.registerTable(TABLE, "metadata_location_without_slashes"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid metadata file location");
  }

  @Test
  public void testConcurrencyConflictCreateTableUpdatedDuringFinalTransaction() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";

    // Use a spy so that non-transactional pre-requisites succeed normally, but we inject
    // a concurrency failure at final commit.
    PolarisMetaStoreManager spyMetaStore = spy(metaStoreManager);
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, CATALOG_NAME);
    final IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            spyMetaStore,
            polarisContext,
            passthroughView,
            securityContext,
            Mockito.mock(TaskExecutor.class),
            fileIOFactory,
            polarisEventListener);
    catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    final TableIdentifier table = TableIdentifier.of(namespace, "conflict_table");

    doReturn(
            new EntityResult(
                BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS,
                PolarisEntitySubType.ICEBERG_TABLE.getCode()))
        .when(spyMetaStore)
        .createEntityIfNotExists(any(), any(), any());
    Assertions.assertThatThrownBy(() -> catalog.createTable(table, SCHEMA))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("conflict_table");
  }

  @Test
  public void testConcurrencyConflictUpdateTableDuringFinalTransaction() {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");
    Assumptions.assumeTrue(
        supportsNestedNamespaces(), "Only applicable if nested namespaces are supported");

    final String tableLocation = "s3://externally-owned-bucket/table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";

    // Use a spy so that non-transactional pre-requisites succeed normally, but we inject
    // a concurrency failure at final commit.
    PolarisMetaStoreManager spyMetaStore = spy(metaStoreManager);
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            polarisContext, entityManager, securityContext, CATALOG_NAME);
    final IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            spyMetaStore,
            polarisContext,
            passthroughView,
            securityContext,
            Mockito.mock(TaskExecutor.class),
            fileIOFactory,
            polarisEventListener);
    catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
    Namespace namespace = Namespace.of("parent", "child1");

    createNonExistingNamespaces(namespace);

    final TableIdentifier tableId = TableIdentifier.of(namespace, "conflict_table");

    Table table = catalog.buildTable(tableId, SCHEMA).create();

    doReturn(new EntityResult(BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED, null))
        .when(spyMetaStore)
        .updateEntityPropertiesIfNotChanged(any(), any(), any());

    UpdateSchema update = table.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expected = update.apply();

    Assertions.assertThatThrownBy(() -> update.commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("conflict_table");
  }

  @Test
  public void createCatalogWithReservedProperty() {
    Assertions.assertThatCode(
            () -> {
              adminService.createCatalog(
                  new CreateCatalogRequest(
                      toCatalog(
                          new CatalogEntity.Builder()
                              .setDefaultBaseLocation("file://")
                              .setName("createCatalogWithReservedProperty")
                              .setProperties(ImmutableMap.of("polaris.reserved", "true"))
                              .build())));
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved prefix");
  }

  @Test
  public void updateCatalogWithReservedProperty() {
    adminService.createCatalog(
        new CreateCatalogRequest(
            toCatalog(
                new CatalogEntity.Builder()
                    .setDefaultBaseLocation("file://")
                    .setName("updateCatalogWithReservedProperty")
                    .setProperties(ImmutableMap.of("a", "b"))
                    .build())));
    Assertions.assertThatCode(
            () -> {
              adminService.updateCatalog(
                  "updateCatalogWithReservedProperty",
                  UpdateCatalogRequest.builder()
                      .setCurrentEntityVersion(1)
                      .setProperties(ImmutableMap.of("polaris.reserved", "true"))
                      .build());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved prefix");
    adminService.deleteCatalog("updateCatalogWithReservedProperty");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testTableOperationsDoesNotRefreshAfterCommit(boolean updateMetadataOnCommit) {
    Assumptions.assumeTrue(
        requiresNamespaceCreate(),
        "Only applicable if namespaces must be created before adding children");

    catalog.createNamespace(NS);
    catalog.buildTable(TABLE, SCHEMA).create();

    IcebergCatalog.BasePolarisTableOperations realOps =
        (IcebergCatalog.BasePolarisTableOperations)
            catalog.newTableOps(TABLE, updateMetadataOnCommit);
    IcebergCatalog.BasePolarisTableOperations ops = Mockito.spy(realOps);

    try (MockedStatic<TableMetadataParser> mocked =
        Mockito.mockStatic(TableMetadataParser.class, Mockito.CALLS_REAL_METHODS)) {
      TableMetadata base1 = ops.current();
      mocked.verify(
          () -> TableMetadataParser.read(Mockito.any(), Mockito.anyString()), Mockito.times(1));

      TableMetadata base2 = ops.refresh();
      mocked.verify(
          () -> TableMetadataParser.read(Mockito.any(), Mockito.anyString()), Mockito.times(1));

      Assertions.assertThat(base1.metadataFileLocation()).isEqualTo(base2.metadataFileLocation());
      Assertions.assertThat(base1).isEqualTo(base2);

      Schema newSchema =
          new Schema(Types.NestedField.optional(100, "new_col", Types.LongType.get()));
      TableMetadata newMetadata =
          TableMetadata.buildFrom(base1).setCurrentSchema(newSchema, 100).build();
      ops.commit(base2, newMetadata);
      mocked.verify(
          () -> TableMetadataParser.read(Mockito.any(), Mockito.anyString()), Mockito.times(1));

      ops.current();
      int expectedReads = updateMetadataOnCommit ? 1 : 2;
      mocked.verify(
          () -> TableMetadataParser.read(Mockito.any(), Mockito.anyString()),
          Mockito.times(expectedReads));
      ops.refresh();
      mocked.verify(
          () -> TableMetadataParser.read(Mockito.any(), Mockito.anyString()),
          Mockito.times(expectedReads));
    } finally {
      catalog.dropTable(TABLE, true);
    }
  }

  @Test
  public void testEventsAreEmitted() {
    IcebergCatalog catalog = catalog();
    catalog.createNamespace(TestData.NAMESPACE);
    Table table = catalog.buildTable(TestData.TABLE, TestData.SCHEMA).create();

    String key = "foo";
    String valOld = "bar1";
    String valNew = "bar2";
    table.updateProperties().set(key, valOld).commit();
    table.updateProperties().set(key, valNew).commit();

    var beforeRefreshEvent = testPolarisEventListener.getLatest(BeforeTableRefreshedEvent.class);
    Assertions.assertThat(beforeRefreshEvent.tableIdentifier()).isEqualTo(TestData.TABLE);

    var afterRefreshEvent = testPolarisEventListener.getLatest(AfterTableRefreshedEvent.class);
    Assertions.assertThat(afterRefreshEvent.tableIdentifier()).isEqualTo(TestData.TABLE);

    var beforeTableEvent = testPolarisEventListener.getLatest(BeforeTableCommitedEvent.class);
    Assertions.assertThat(beforeTableEvent.identifier()).isEqualTo(TestData.TABLE);
    Assertions.assertThat(beforeTableEvent.base().properties().get(key)).isEqualTo(valOld);
    Assertions.assertThat(beforeTableEvent.metadata().properties().get(key)).isEqualTo(valNew);

    var afterTableEvent = testPolarisEventListener.getLatest(AfterTableCommitedEvent.class);
    Assertions.assertThat(afterTableEvent.identifier()).isEqualTo(TestData.TABLE);
    Assertions.assertThat(afterTableEvent.base().properties().get(key)).isEqualTo(valOld);
    Assertions.assertThat(afterTableEvent.metadata().properties().get(key)).isEqualTo(valNew);
  }
}
