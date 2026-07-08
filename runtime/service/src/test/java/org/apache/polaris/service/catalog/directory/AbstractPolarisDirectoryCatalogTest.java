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
package org.apache.polaris.service.catalog.directory;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.table.DirectoryEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.context.catalog.PolarisPrincipalHolder;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public abstract class AbstractPolarisDirectoryCatalogTest {

  protected static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  public static final String CATALOG_NAME = "polaris-catalog";
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;
  @Inject ResolverFactory resolverFactory;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject UserSecretsManager userSecretsManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;
  @Inject PolarisPrincipalHolder polarisPrincipalHolder;

  private PolarisDirectoryCatalog directoryCatalog;
  private IcebergCatalog icebergCatalog;
  private AwsStorageConfigInfo storageConfigModel;
  private String realmName;
  private PolarisCallContext polarisContext;
  private PolarisAdminService adminService;
  private PolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;

  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  protected void bootstrapRealm(String realmName) {}

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    bootstrapRealm(realmName);

    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    polarisContext = callContext.getPolarisCallContext();

    PrincipalEntity rootPrincipal =
        metaStoreManager.findRootPrincipal(polarisContext).orElseThrow();
    authenticatedRoot = PolarisPrincipal.of(rootPrincipal, Set.of());
    polarisPrincipalHolder.set(authenticatedRoot);

    PolarisAuthorizer authorizer = new PolarisAuthorizerImpl(realmConfig);
    ReservedProperties reservedProperties = ReservedProperties.NONE;

    adminService =
        new PolarisAdminService(
            polarisContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            authenticatedRoot,
            authorizer,
            reservedProperties);

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
            new CreateCatalogRequest(
                new CatalogEntity.Builder()
                    .setName(CATALOG_NAME)
                    .setDefaultBaseLocation(storageLocation)
                    .addProperty(
                        FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                    .addProperty(
                        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                        "true")
                    .addProperty(
                        FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true")
                    .setStorageConfigurationInfo(realmConfig, storageConfigModel, storageLocation)
                    .build()
                    .asCatalog(serviceIdentityProvider)));

    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, authenticatedRoot, CATALOG_NAME);
    TaskExecutor taskExecutor = Mockito.mock();

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
        new AwsCredentialsStorageIntegration(
            (AwsStorageConfigurationInfo)
                CatalogEntity.of(catalogEntity).getStorageConfigurationInfo(),
            stsClient);
    when(storageIntegrationProvider.getStorageIntegrationForConfig(
            isA(AwsStorageConfigurationInfo.class)))
        .thenReturn((PolarisStorageIntegration) storageIntegration);

    this.directoryCatalog =
        new PolarisDirectoryCatalog(metaStoreManager, polarisContext, passthroughView);
    this.directoryCatalog.initialize(CATALOG_NAME, Map.of());
    this.icebergCatalog =
        new IcebergCatalog(
            diagServices,
            resolverFactory,
            metaStoreManager,
            polarisContext,
            passthroughView,
            authenticatedRoot,
            taskExecutor,
            storageAccessConfigProvider,
            fileIOFactory,
            new InMemoryEventCollector(),
            eventMetadataFactory);
    this.icebergCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  @AfterEach
  public void after() throws IOException {
    metaStoreManager.purge(polarisContext);
  }

  @Test
  public void testCreateDirectoryDoesNotThrow() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    Assertions.assertThatCode(
            () ->
                directoryCatalog.createDirectory(
                    TableIdentifier.of("ns", "d1"),
                    "s3://bucket/path",
                    null,
                    null,
                    null,
                    Map.of()))
        .doesNotThrowAnyException();
  }

  @Test
  public void testCreateAndLoadDirectory() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    directoryCatalog.createDirectory(
        TableIdentifier.of("ns", "d1"), "s3://bucket/path", null, null, null, Map.of());

    DirectoryEntity entity = directoryCatalog.loadDirectory(TableIdentifier.of("ns", "d1"));
    Assertions.assertThat(entity.getName()).isEqualTo("d1");
    Assertions.assertThat(entity.getBaseLocation()).isEqualTo("s3://bucket/path");
  }

  @Test
  public void testDirectoryAlreadyExists() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    directoryCatalog.createDirectory(
        TableIdentifier.of("ns", "d1"), "s3://bucket/path", null, null, null, Map.of());

    Assertions.assertThatCode(
            () ->
                directoryCatalog.createDirectory(
                    TableIdentifier.of("ns", "d1"),
                    "s3://bucket/other",
                    null,
                    null,
                    null,
                    Map.of()))
        .hasMessageContaining("already exists");

    Assertions.assertThatCode(
            () -> icebergCatalog.createTable(TableIdentifier.of("ns", "d1"), SCHEMA))
        .hasMessageContaining("already exists");
  }

  @Test
  public void testIcebergTableAlreadyExists() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    icebergCatalog.createTable(TableIdentifier.of("ns", "t1"), SCHEMA);

    Assertions.assertThatCode(
            () ->
                directoryCatalog.createDirectory(
                    TableIdentifier.of("ns", "t1"),
                    "s3://bucket/path",
                    null,
                    null,
                    null,
                    Map.of()))
        .hasMessageContaining("already exists");
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"{\"cron\":\"0 * * * *\"}", "{\"cron\":\"0 0 * * *\"}"})
  public void testDirectoryRoundTrip(String scanSchedule) {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String directoryName = "d1";
    String baseLocation = "s3://bucket/images/";
    Map<String, String> properties = Map.of("a", "b", "c", "d");
    String filterInclude = "[\".*\\\\.jpg$\"]";
    String filterExclude = "[\".*thumbs/.*\"]";

    directoryCatalog.createDirectory(
        TableIdentifier.of("ns", directoryName),
        baseLocation,
        filterInclude,
        filterExclude,
        scanSchedule,
        properties);

    DirectoryEntity resultEntity =
        directoryCatalog.loadDirectory(TableIdentifier.of("ns", directoryName));

    Assertions.assertThat(resultEntity.getBaseLocation()).isEqualTo(baseLocation);
    Assertions.assertThat(resultEntity.getFilterInclude()).isEqualTo(filterInclude);
    Assertions.assertThat(resultEntity.getFilterExclude()).isEqualTo(filterExclude);
    Assertions.assertThat(resultEntity.getScanSchedule()).isEqualTo(scanSchedule);
    Assertions.assertThat(resultEntity.getPropertiesAsMap()).isEqualTo(properties);
    Assertions.assertThat(resultEntity.getName()).isEqualTo(directoryName);
  }

  @Test
  public void testLoadNonExistentDirectory() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    Assertions.assertThatCode(() -> directoryCatalog.loadDirectory(TableIdentifier.of("ns", "d1")))
        .hasMessageContaining("does not exist: ns.d1");
  }

  @Test
  public void testReadIcebergTableAsDirectory() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";

    icebergCatalog.createTable(TableIdentifier.of("ns", tableName), SCHEMA);
    Assertions.assertThatCode(
            () -> directoryCatalog.loadDirectory(TableIdentifier.of("ns", tableName)))
        .hasMessageContaining("does not exist: ns.t1");
  }

  @Test
  public void testListDirectories() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      directoryCatalog.createDirectory(
          TableIdentifier.of("ns", "d" + i),
          "s3://bucket/path" + i,
          null,
          null,
          null,
          Map.of());
    }

    List<TableIdentifier> listResult = directoryCatalog.listDirectories(namespace);

    Assertions.assertThat(listResult.size()).isEqualTo(10);
    Assertions.assertThat(listResult.stream().map(TableIdentifier::toString).toList())
        .isEqualTo(listResult.stream().map(TableIdentifier::toString).sorted().toList());

    // Iceberg tables list should NOT include the directory entities themselves
    // (they are DIRECTORY subtype, not ICEBERG_TABLE)
    // but it should be empty since we haven't created any regular Iceberg tables
    Assertions.assertThat(icebergCatalog.listTables(namespace)).isEmpty();
  }

  @Test
  public void testListDirectoriesEmpty() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      icebergCatalog.createTable(TableIdentifier.of("ns", "t" + i), SCHEMA);
    }

    Assertions.assertThat(icebergCatalog.listTables(namespace).size()).isEqualTo(10);
    Assertions.assertThat(directoryCatalog.listDirectories(namespace)).isEmpty();
  }

  @Test
  public void testListDirectoriesNoNamespace() {
    Namespace namespace = Namespace.of("ns");

    Assertions.assertThatCode(() -> directoryCatalog.listDirectories(namespace))
        .hasMessageContaining("Namespace");
  }

  @Test
  public void testDropNonExistentDirectory() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    Assertions.assertThatCode(
            () -> directoryCatalog.dropDirectory(TableIdentifier.of("ns", "d1")))
        .hasMessageContaining("Directory does not exist: ns.d1");
  }

  @Test
  public void testDropNonExistentNamespace() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    Assertions.assertThatCode(
            () -> directoryCatalog.dropDirectory(TableIdentifier.of("ns2", "d1")))
        .hasMessageContaining("Directory does not exist: ns2.d1");
  }

  @Test
  public void testDropIcebergTable() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    icebergCatalog.createTable(TableIdentifier.of("ns", "t1"), SCHEMA);

    Assertions.assertThatCode(
            () -> directoryCatalog.dropDirectory(TableIdentifier.of("ns", "t1")))
        .hasMessageContaining("Directory does not exist: ns.t1");

    Assertions.assertThatCode(() -> icebergCatalog.dropTable(TableIdentifier.of("ns", "t1")))
        .doesNotThrowAnyException();
  }

  @Test
  public void testDropViaIceberg() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    directoryCatalog.createDirectory(
        TableIdentifier.of("ns", "d1"), "s3://bucket/path", null, null, null, Map.of());

    Assertions.assertThat(icebergCatalog.dropTable(TableIdentifier.of("ns", "d1"))).isFalse();
    Assertions.assertThat(directoryCatalog.loadDirectory(TableIdentifier.of("ns", "d1")))
        .isNotNull();
  }

  @Test
  public void testListMixedTables() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      icebergCatalog.createTable(TableIdentifier.of("ns", "i" + i), SCHEMA);
    }

    for (int i = 0; i < 10; i++) {
      directoryCatalog.createDirectory(
          TableIdentifier.of("ns", "d" + i),
          "s3://bucket/path" + i,
          null,
          null,
          null,
          Map.of());
    }

    Assertions.assertThat(directoryCatalog.listDirectories(namespace).size()).isEqualTo(10);
    Assertions.assertThat(icebergCatalog.listTables(namespace).size()).isEqualTo(10);
  }
}
