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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
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
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.table.GenericTableEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.secrets.UserSecretsManagerFactory;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.iceberg.IcebergCatalog;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.listeners.NoOpPolarisEventListener;
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

public abstract class AbstractPolarisGenericTableCatalogTest {

  protected static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  public static final String CATALOG_NAME = "polaris-catalog";
  public static final String TEST_ACCESS_KEY = "test_access_key";
  public static final String SECRET_ACCESS_KEY = "secret_access_key";
  public static final String SESSION_TOKEN = "session_token";

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject UserSecretsManagerFactory userSecretsManagerFactory;
  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject PolarisConfigurationStore configurationStore;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject PolarisDiagnostics diagServices;
  @Inject ResolverFactory resolverFactory;
  @Inject ResolutionManifestFactory resolutionManifestFactory;

  private PolarisGenericTableCatalog genericTableCatalog;
  private IcebergCatalog icebergCatalog;
  private AwsStorageConfigInfo storageConfigModel;
  private String realmName;
  private PolarisMetaStoreManager metaStoreManager;
  private UserSecretsManager userSecretsManager;
  private PolarisCallContext polarisContext;
  private RealmConfig realmConfig;
  private PolarisAdminService adminService;
  private FileIOFactory fileIOFactory;
  private PolarisPrincipal authenticatedRoot;
  private PolarisEntity catalogEntity;
  private SecurityContext securityContext;
  private StorageAccessConfigProvider storageAccessConfigProvider;

  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
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
    metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);
    userSecretsManager = userSecretsManagerFactory.getOrCreateUserSecretsManager(realmContext);
    polarisContext =
        new PolarisCallContext(
            realmContext,
            metaStoreManagerFactory.getOrCreateSession(realmContext),
            configurationStore);
    realmConfig = polarisContext.getRealmConfig();
    storageAccessConfigProvider =
        new StorageAccessConfigProvider(storageCredentialCache, metaStoreManagerFactory);

    PrincipalEntity rootPrincipal =
        metaStoreManager.findRootPrincipal(polarisContext).orElseThrow();
    authenticatedRoot = PolarisPrincipal.of(rootPrincipal, Set.of());

    securityContext = Mockito.mock(SecurityContext.class);
    when(securityContext.getUserPrincipal()).thenReturn(authenticatedRoot);
    when(securityContext.isUserInRole(isA(String.class))).thenReturn(true);

    PolarisAuthorizer authorizer = new PolarisAuthorizerImpl(realmConfig);
    ReservedProperties reservedProperties = ReservedProperties.NONE;

    adminService =
        new PolarisAdminService(
            diagServices,
            polarisContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            securityContext,
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
                    .setReplaceNewLocationPrefixWithCatalogDefault("file:")
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
            resolutionManifestFactory, securityContext, CATALOG_NAME);
    TaskExecutor taskExecutor = Mockito.mock();
    this.fileIOFactory = new DefaultFileIOFactory(storageAccessConfigProvider);

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

    this.genericTableCatalog =
        new PolarisGenericTableCatalog(metaStoreManager, polarisContext, passthroughView);
    this.genericTableCatalog.initialize(CATALOG_NAME, Map.of());
    this.icebergCatalog =
        new IcebergCatalog(
            diagServices,
            resolverFactory,
            metaStoreManager,
            polarisContext,
            passthroughView,
            securityContext,
            taskExecutor,
            fileIOFactory,
            new NoOpPolarisEventListener());
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
  public void testCreateGenericTableDoesNotThrow() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    Assertions.assertThatCode(
            () ->
                genericTableCatalog.createGenericTable(
                    TableIdentifier.of("ns", "t1"), "test-format", null, "doc", Map.of()))
        .doesNotThrowAnyException();
  }

  @Test
  public void testGenericTableAlreadyExists() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    genericTableCatalog.createGenericTable(
        TableIdentifier.of("ns", "t1"), "format1", null, "doc", Map.of());

    Assertions.assertThatCode(
            () ->
                genericTableCatalog.createGenericTable(
                    TableIdentifier.of("ns", "t1"), "format2", null, "doc", Map.of()))
        .hasMessageContaining("already exists");

    Assertions.assertThatCode(
            () -> icebergCatalog.createTable(TableIdentifier.of("ns", "t1"), SCHEMA))
        .hasMessageContaining("already exists");
  }

  @Test
  public void testIcebergTableAlreadyExists() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    icebergCatalog.createTable(TableIdentifier.of("ns", "t1"), SCHEMA);

    Assertions.assertThatCode(
            () ->
                genericTableCatalog.createGenericTable(
                    TableIdentifier.of("ns", "t1"), "format2", null, "doc", Map.of()))
        .hasMessageContaining("already exists");

    Assertions.assertThatCode(
            () -> icebergCatalog.createTable(TableIdentifier.of("ns", "t1"), SCHEMA))
        .hasMessageContaining("already exists");
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "file://path/to/my/table"})
  public void testGenericTableRoundTrip(String baseLocation) {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";
    Map<String, String> properties = Map.of("a", "b", "c", "d");
    String format = "round-trip-format";
    String doc = "round-trip-doc";

    genericTableCatalog.createGenericTable(
        TableIdentifier.of("ns", tableName), format, baseLocation, doc, properties);

    GenericTableEntity resultEntity =
        genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", tableName));

    Assertions.assertThat(resultEntity.getFormat()).isEqualTo(format);
    Assertions.assertThat(resultEntity.getPropertiesAsMap()).isEqualTo(properties);
    Assertions.assertThat(resultEntity.getName()).isEqualTo(tableName);
    Assertions.assertThat(resultEntity.getBaseLocation()).isEqualTo(baseLocation);
  }

  @Test
  public void testLoadNonExistentTable() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    Assertions.assertThatCode(
            () -> genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", "t1")))
        .hasMessageContaining("does not exist: ns.t1");
  }

  @Test
  public void testReadIcebergTableAsGeneric() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";

    icebergCatalog.createTable(TableIdentifier.of("ns", tableName), SCHEMA);
    Assertions.assertThatCode(
            () -> genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", tableName)))
        .hasMessageContaining("does not exist: ns.t1");
  }

  @Test
  public void testReadIcebergViewAsGeneric() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";

    icebergCatalog.buildView(TableIdentifier.of("ns", tableName));
    Assertions.assertThatCode(
            () -> genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", tableName)))
        .hasMessageContaining("does not exist: ns.t1");
  }

  @Test
  public void testReadGenericAsIcebergTable() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";

    genericTableCatalog.createGenericTable(
        TableIdentifier.of("ns", tableName), "format", null, "doc", Map.of());
    Assertions.assertThatCode(() -> icebergCatalog.loadTable(TableIdentifier.of("ns", tableName)))
        .hasMessageContaining("does not exist: ns.t1");
  }

  @Test
  public void testReadGenericAsIcebergView() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    String tableName = "t1";

    genericTableCatalog.createGenericTable(
        TableIdentifier.of("ns", tableName), "format", null, "doc", Map.of());
    Assertions.assertThatCode(() -> icebergCatalog.loadView(TableIdentifier.of("ns", tableName)))
        .hasMessageContaining("does not exist: ns.t1");
  }

  @Test
  public void testListTables() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      genericTableCatalog.createGenericTable(
          TableIdentifier.of("ns", "t" + i), "format", null, "doc", Map.of());
    }

    List<TableIdentifier> listResult = genericTableCatalog.listGenericTables(namespace);

    Assertions.assertThat(listResult.size()).isEqualTo(10);
    Assertions.assertThat(listResult.stream().map(TableIdentifier::toString).toList())
        .isEqualTo(listResult.stream().map(TableIdentifier::toString).sorted().toList());

    Assertions.assertThat(icebergCatalog.listTables(namespace)).isEmpty();
  }

  @Test
  public void testListTablesEmpty() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      icebergCatalog.createTable(TableIdentifier.of("ns", "t" + i), SCHEMA);
    }

    Assertions.assertThat(icebergCatalog.listTables(namespace).size()).isEqualTo(10);
    Assertions.assertThat(genericTableCatalog.listGenericTables(namespace)).isEmpty();
  }

  @Test
  public void testListTablesNoNamespace() {
    Namespace namespace = Namespace.of("ns");

    Assertions.assertThatCode(() -> genericTableCatalog.listGenericTables(namespace))
        .hasMessageContaining("Namespace");
  }

  @Test
  public void testListIcebergTables() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      icebergCatalog.createTable(TableIdentifier.of("ns", "t" + i), SCHEMA);
    }

    List<TableIdentifier> listResult = icebergCatalog.listTables(namespace);

    Assertions.assertThat(listResult.size()).isEqualTo(10);
    Assertions.assertThat(listResult.stream().map(TableIdentifier::toString).toList())
        .isEqualTo(listResult.stream().map(TableIdentifier::toString).sorted().toList());

    Assertions.assertThat(genericTableCatalog.listGenericTables(namespace)).isEmpty();
  }

  @Test
  public void testListMixedTables() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    for (int i = 0; i < 10; i++) {
      icebergCatalog.createTable(TableIdentifier.of("ns", "i" + i), SCHEMA);
    }

    for (int i = 0; i < 10; i++) {
      genericTableCatalog.createGenericTable(
          TableIdentifier.of("ns", "g" + i), "format", null, "doc", Map.of());
    }

    Assertions.assertThat(genericTableCatalog.listGenericTables(namespace).size()).isEqualTo(10);
    Assertions.assertThat(icebergCatalog.listTables(namespace).size()).isEqualTo(10);
  }

  @Test
  public void testDropNonExistentTable() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    Assertions.assertThatCode(
            () -> genericTableCatalog.dropGenericTable(TableIdentifier.of("ns", "t1")))
        .hasMessageContaining("Generic table does not exist: ns.t1");
  }

  @Test
  public void testDropNonExistentNamespace() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);

    Assertions.assertThatCode(
            () -> genericTableCatalog.dropGenericTable(TableIdentifier.of("ns2", "t1")))
        .hasMessageContaining("Generic table does not exist: ns2.t1");
  }

  @Test
  public void testDropIcebergTable() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    icebergCatalog.createTable(TableIdentifier.of("ns", "t1"), SCHEMA);

    Assertions.assertThatCode(
            () -> genericTableCatalog.dropGenericTable(TableIdentifier.of("ns", "t1")))
        .hasMessageContaining("Generic table does not exist: ns.t1");

    Assertions.assertThatCode(() -> icebergCatalog.dropTable(TableIdentifier.of("ns", "t1")))
        .doesNotThrowAnyException();
  }

  @Test
  public void testDropViaIceberg() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    genericTableCatalog.createGenericTable(
        TableIdentifier.of("ns", "t1"), "format", null, "doc", Map.of());

    Assertions.assertThat(icebergCatalog.dropTable(TableIdentifier.of("ns", "t1"))).isFalse();
    Assertions.assertThat(genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", "t1")))
        .isNotNull();
  }

  @Test
  public void testDropViaIcebergView() {
    Namespace namespace = Namespace.of("ns");
    icebergCatalog.createNamespace(namespace);
    genericTableCatalog.createGenericTable(
        TableIdentifier.of("ns", "t1"), "format", null, "doc", Map.of());

    Assertions.assertThat(icebergCatalog.dropView(TableIdentifier.of("ns", "t1"))).isFalse();
    Assertions.assertThat(genericTableCatalog.loadGenericTable(TableIdentifier.of("ns", "t1")))
        .isNotNull();
  }
}
