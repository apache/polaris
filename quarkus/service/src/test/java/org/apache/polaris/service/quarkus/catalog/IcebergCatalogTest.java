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
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.azure.core.exception.HttpResponseException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
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
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
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
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.catalog.io.ExceptionMappingFileIO;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.MeasuredFileIOFactory;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.exception.FakeAzureHttpResponse;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TableCleanupTaskHandler;
import org.apache.polaris.service.task.TaskExecutor;
import org.apache.polaris.service.task.TaskFileIOSupplier;
import org.apache.polaris.service.types.NotificationRequest;
import org.apache.polaris.service.types.NotificationType;
import org.apache.polaris.service.types.TableUpdateNotification;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

@TestProfile(IcebergCatalogTest.Profile.class)
public abstract class IcebergCatalogTest extends CatalogTests<IcebergCatalog> {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.features.defaults.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"",
          "true",
          "polaris.features.defaults.\"INITIALIZE_DEFAULT_CATALOG_FILEIO_FOR_TEST\"",
          "true",
          "polaris.features.defaults.\"ALLOW_OVERLAPPING_CATALOG_URLS\"",
          "true",
          "polaris.features.defaults.\"SUPPORTED_CATALOG_STORAGE_TYPES\"",
          "[\"FILE\"]");
    }
  }

  protected static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(4, "data", Types.StringType.get()));
  public static final String CATALOG_NAME = "polaris-catalog";
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  @Inject MetaStoreManagerFactory managerFactory;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;

  private IcebergCatalog catalog;
  private CallContext callContext;
  private AwsStorageConfigInfo storageConfigModel;
  private String realmName;
  private PolarisMetaStoreManager metaStoreManager;
  private PolarisCallContext polarisContext;
  private PolarisAdminService adminService;
  private PolarisEntityManager entityManager;
  private FileIOFactory fileIOFactory;
  private AuthenticatedPolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;
  private SecurityContext securityContext;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @Nullable
  protected abstract EntityCache createEntityCache(PolarisMetaStoreManager metaStoreManager);

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    RealmContext realmContext = () -> realmName;
    metaStoreManager = managerFactory.getOrCreateMetaStoreManager(realmContext);
    polarisContext =
        new PolarisCallContext(
            managerFactory.getOrCreateSessionSupplier(realmContext).get(),
            diagServices,
            configurationStore,
            Clock.systemDefaultZone());
    entityManager =
        new PolarisEntityManager(
            metaStoreManager, new StorageCredentialCache(), createEntityCache(metaStoreManager));

    callContext = CallContext.of(realmContext, polarisContext);

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

    authenticatedRoot = new AuthenticatedPolarisPrincipal(rootEntity, Set.of());

    securityContext = Mockito.mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(authenticatedRoot);
    when(securityContext.isUserInRole(isA(String.class))).thenReturn(true);
    adminService =
        new PolarisAdminService(
            callContext,
            entityManager,
            metaStoreManager,
            securityContext,
            new PolarisAuthorizerImpl(new PolarisConfigurationStore() {}));

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

    this.catalog = initCatalog(CATALOG_NAME, ImmutableMap.of());
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

  @Override
  protected IcebergCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    String storageLocation = "s3://my-bucket/path/to/data";
    storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(storageLocation, "s3://externally-owned-bucket"))
            .build();
    catalogEntity =
        adminService.createCatalog(
            new CatalogEntity.Builder()
                .setName(catalogName)
                .setDefaultBaseLocation(storageLocation)
                .setReplaceNewLocationPrefixWithCatalogDefault("file:")
                .addProperty(
                    FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                .addProperty(
                    FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
                .setStorageConfigurationInfo(storageConfigModel, storageLocation)
                .build());
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, catalogName);
    TaskExecutor taskExecutor = Mockito.mock();
    IcebergCatalog icebergCatalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory);

    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putAll(additionalProperties);
    icebergCatalog.initialize(catalogName, propertiesBuilder.buildKeepingLast());
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
        return new StorageCredentialCache();
      }

      @Override
      public EntityCache getOrCreateEntityCache(RealmContext realmContext) {
        return new EntityCache(metaStoreManager);
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
  @Override
  public void listNamespacesWithEmptyNamespace() {
    // TODO: remove this override test once Polaris handles empty namespaces the same as the
    // superclass expectation.
    Assumptions.assumeTrue(supportsEmptyNamespace());
    super.listNamespacesWithEmptyNamespace();
  }

  @Test
  @Override
  public void createAndDropEmptyNamespace() {
    // Skip this test because AssertJ's Assumptions.assumeThat() is not compatible with Quarkus.
    // This test can be removed once Quarkus supports AssertJ or Polaris supports empty namespaces.
    Assumptions.assumeTrue(supportsEmptyNamespace());
    super.createAndDropEmptyNamespace();
  }

  @Test
  @Override
  public void namespacePropertiesOnEmptyNamespace() {
    // Skip this test because AssertJ's Assumptions.assumeThat() is not compatible with Quarkus.
    // This test can be removed once Quarkus supports AssertJ or Polaris supports empty namespaces.
    Assumptions.assumeTrue(supportsEmptyNamespace());
    super.namespacePropertiesOnEmptyNamespace();
  }

  @Test
  @Override
  public void listTablesInEmptyNamespace() {
    // Skip this test because AssertJ's Assumptions.assumeThat() is not compatible with Quarkus.
    // This test can be removed once Quarkus supports AssertJ or Polaris supports empty namespaces.
    Assumptions.assumeTrue(supportsEmptyNamespace());
    super.listTablesInEmptyNamespace();
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
    InMemoryFileIO fileIO = getInMemoryIo(catalog);
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
            callContext, entityManager, securityContext, catalog().name());
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
            callContext,
            passthroughView,
            securityContext,
            Mockito.mock(TaskExecutor.class),
            fileIOFactory);
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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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
    final String tableLocation = "s3://my-bucket/path/to/data/my_table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    final String anotherTableLocation = "s3://my-bucket/path/to/data/another_table/";

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
    TableMetadataParser.write(tableMetadata, catalog.getIo().newOutputFile(tableMetadataLocation));

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

    // The location of the metadata JSON file specified is outside of the table's metadata directory
    // according to the
    // metadata. We assume this is fraudulent and disallowed
    final String tableLocation = "s3://my-bucket/path/to/data/my_table/";
    final String tableMetadataLocation = tableLocation + "metadata/v3.metadata.json";

    // this passes the first validation, since it's within the namespace subdirectory, but
    // the location is in another table's subdirectory
    final String anotherTableLocation = "s3://my-bucket/path/to/data/another_table";

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
    TableMetadataParser.write(tableMetadata, catalog.getIo().newOutputFile(tableMetadataLocation));

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
    final String tableLocation = "s3://my-bucket/path/to/data/my_table/";
    final String tableMetadataLocation = tableLocation + "metadata/v1.metadata.json";
    final String anotherTableLocation = "s3://my-bucket/path/to/data/another_table/";

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
    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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
    TableMetadataParser.write(tableMetadata, catalog.getIo().newOutputFile(maliciousMetadataFile));

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
            new CatalogEntity.Builder()
                .setDefaultBaseLocation("file://")
                .setName(catalogWithoutStorage)
                .build());

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, catalogWithoutStorage);
    TaskExecutor taskExecutor = Mockito.mock();
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory);
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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

    fileIO.addFile(
        metadataLocation,
        TableMetadataParser.toJson(createSampleTableMetadata(metadataLocation)).getBytes(UTF_8));

    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    if (!polarisCallContext
        .getConfigurationStore()
        .getConfiguration(polarisCallContext, FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES)
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
        new CatalogEntity.Builder()
            .setDefaultBaseLocation("http://maliciousdomain.com")
            .setName(catalogName)
            .build());

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, catalogName);
    TaskExecutor taskExecutor = Mockito.mock();
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory);
    catalog.initialize(
        catalogName,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Namespace namespace = Namespace.of("parent", "child1");
    TableIdentifier table = TableIdentifier.of(namespace, "table");

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    PolarisCallContext polarisCallContext = callContext.getPolarisCallContext();
    if (!polarisCallContext
        .getConfigurationStore()
        .getConfiguration(polarisCallContext, FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES)
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

    if (!polarisCallContext
        .getConfigurationStore()
        .getConfiguration(polarisCallContext, FeatureConfiguration.SUPPORTED_CATALOG_STORAGE_TYPES)
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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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

    InMemoryFileIO fileIO = getInMemoryIo(catalog);

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
        metaStoreManager.loadTasks(polarisContext, "testExecutor", 1).getEntities();
    Assertions.assertThat(tasks).hasSize(1);
    TaskEntity taskEntity = TaskEntity.of(tasks.get(0));
    EnumMap<PolarisCredentialProperty, String> credentials =
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
        .containsEntry(PolarisCredentialProperty.AWS_KEY_ID, TEST_ACCESS_KEY)
        .containsEntry(PolarisCredentialProperty.AWS_SECRET_KEY, SECRET_ACCESS_KEY)
        .containsEntry(PolarisCredentialProperty.AWS_TOKEN, SESSION_TOKEN);
    MetaStoreManagerFactory metaStoreManagerFactory = createMockMetaStoreManagerFactory();
    FileIO fileIO =
        new TaskFileIOSupplier(
                new DefaultFileIOFactory(
                    new RealmEntityManagerFactory(metaStoreManagerFactory),
                    metaStoreManagerFactory,
                    configurationStore))
            .apply(taskEntity, callContext);
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
        new CatalogEntity.Builder()
            .setName(noPurgeCatalogName)
            .setDefaultBaseLocation(storageLocation)
            .setReplaceNewLocationPrefixWithCatalogDefault("file:")
            .addProperty(FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
            .addProperty(
                FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
            .addProperty(FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "false")
            .setStorageConfigurationInfo(noPurgeStorageConfigModel, storageLocation)
            .build());
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            callContext, entityManager, securityContext, noPurgeCatalogName);
    IcebergCatalog noPurgeCatalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            Mockito.mock(),
            fileIOFactory);
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
            callContext, entityManager, securityContext, CATALOG_NAME);

    MeasuredFileIOFactory measured =
        new MeasuredFileIOFactory(
            new RealmEntityManagerFactory(createMockMetaStoreManagerFactory()),
            managerFactory,
            configurationStore);
    IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            metaStoreManager,
            callContext,
            passthroughView,
            securityContext,
            Mockito.mock(),
            measured);
    catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    Assertions.assertThat(measured.getNumOutputFiles() + measured.getInputBytes())
        .as("Nothing was created yet")
        .isEqualTo(0);

    catalog.createNamespace(NS);
    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    // Asserting greaterThan 0 is sufficient for validating that the wrapper works without making
    // assumptions about the
    // specific implementations of table operations.
    Assertions.assertThat(measured.getNumOutputFiles()).as("A table was created").isGreaterThan(0);

    table.updateProperties().set("foo", "bar").commit();
    Assertions.assertThat(measured.getInputBytes())
        .as("A table was read and written")
        .isGreaterThan(0);

    Assertions.assertThat(catalog.dropTable(TABLE)).as("Table deletion should succeed").isTrue();
    TaskEntity taskEntity =
        TaskEntity.of(
            metaStoreManager
                .loadTasks(callContext.getPolarisCallContext(), "testExecutor", 1)
                .getEntities()
                .getFirst());
    Map<String, String> properties = taskEntity.getInternalPropertiesAsMap();
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO");
    taskEntity.setInternalPropertiesAsMap(properties);
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
    handler.handleTask(taskEntity, callContext);
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
            callContext, entityManager, securityContext, CATALOG_NAME);
    final IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            spyMetaStore,
            callContext,
            passthroughView,
            securityContext,
            Mockito.mock(TaskExecutor.class),
            fileIOFactory);
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
            callContext, entityManager, securityContext, CATALOG_NAME);
    final IcebergCatalog catalog =
        new IcebergCatalog(
            entityManager,
            spyMetaStore,
            callContext,
            passthroughView,
            securityContext,
            Mockito.mock(TaskExecutor.class),
            fileIOFactory);
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

  private static InMemoryFileIO getInMemoryIo(IcebergCatalog catalog) {
    return (InMemoryFileIO) ((ExceptionMappingFileIO) catalog.getIo()).getInnerIo();
  }
}
