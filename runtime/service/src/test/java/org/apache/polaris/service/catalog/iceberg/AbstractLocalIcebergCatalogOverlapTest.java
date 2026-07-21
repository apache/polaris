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

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import io.smallrye.common.annotation.Identifier;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.AwsStorageConfigInfo;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.context.catalog.RealmContextHolder;
import org.apache.polaris.service.events.PolarisEventDispatcher;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.events.listeners.TestPolarisEventListener;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Base class for persistence-backend-agnostic overlap tests. Concrete subclasses provide a Quarkus
 * test profile with location-overlap enforcement and the optimized sibling check enabled, so the
 * tests exercise the {@code hasOverlappingSiblings} path of whichever metastore implementation the
 * profile wires in.
 */
public abstract class AbstractLocalIcebergCatalogOverlapTest {

  protected static final String CATALOG_NAME = "polaris-catalog";
  protected static final String STORAGE_LOCATION = "s3://my-bucket/path/to/data";
  private static final String TEST_ACCESS_KEY = "test_access_key";
  private static final String SECRET_ACCESS_KEY = "secret_access_key";
  private static final String SESSION_TOKEN = "session_token";

  @Inject Clock clock;
  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject PolarisDiagnostics diagServices;

  @Inject
  @Identifier("test")
  PolarisEventListener polarisEventListener;

  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject RealmContextHolder realmContextHolder;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;
  @Inject PolarisPrincipal authenticatedRoot;
  @Inject PolarisAdminService adminService;
  @Inject ResolverFactory resolverFactory;
  @Inject PolarisEventDispatcher polarisEventDispatcher;

  private LocalIcebergCatalog catalog;
  private String realmName;
  private PolarisCallContext polarisContext;
  private InMemoryFileIO fileIO;
  private PolarisEntity catalogEntity;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    metaStoreManagerFactory
        .bootstrapRealms(
            List.of(realmName),
            RootCredentialsSet.fromList(List.of(realmName + ",aClientId,aSecret")))
        .get(realmName);

    realmContextHolder.set(() -> realmName);
    polarisContext = callContext.getPolarisCallContext();

    AwsStorageConfigInfo storageConfigModel =
        AwsStorageConfigInfo.builder()
            .setRoleArn("arn:aws:iam::012345678901:role/jdoe")
            .setExternalId("externalId")
            .setUserArn("aws::a:user:arn")
            .setStorageType(StorageConfigInfo.StorageTypeEnum.S3)
            .setAllowedLocations(List.of(STORAGE_LOCATION, "s3://externally-owned-bucket"))
            .build();
    catalogEntity =
        adminService.createCatalog(
            new CreateCatalogRequest(
                new CatalogEntity.Builder()
                    .setName(CATALOG_NAME)
                    .setDefaultBaseLocation(STORAGE_LOCATION)
                    .addProperty(
                        FeatureConfiguration.ALLOW_EXTERNAL_METADATA_FILE_LOCATION.catalogConfig(),
                        "true")
                    .addProperty(
                        FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
                        "true")
                    .addProperty(
                        FeatureConfiguration.DROP_WITH_PURGE_ENABLED.catalogConfig(), "true")
                    .setStorageConfigurationInfo(realmConfig, storageConfigModel, STORAGE_LOCATION)
                    .build()
                    .asCatalog(serviceIdentityProvider)));

    StsClient stsClient = Mockito.mock(StsClient.class);
    when(stsClient.assumeRole(Mockito.isA(AssumeRoleRequest.class)))
        .thenReturn(
            AssumeRoleResponse.builder()
                .credentials(
                    Credentials.builder()
                        .accessKeyId(TEST_ACCESS_KEY)
                        .secretAccessKey(SECRET_ACCESS_KEY)
                        .sessionToken(SESSION_TOKEN)
                        .build())
                .build());
    AwsStorageConfigurationInfo mockAwsConfig =
        AwsStorageConfigurationInfo.builder()
            .roleARN("arn:aws:iam::012345678901:role/mock")
            .build();
    AwsCredentialsStorageIntegration storageIntegration =
        new AwsCredentialsStorageIntegration(
            (destination) -> stsClient,
            config -> Optional.empty(),
            storageCredentialCache,
            mockAwsConfig,
            callContext.getRealmConfig());
    when(storageIntegrationProvider.getStorageIntegration(Mockito.anyList()))
        .thenReturn(storageIntegration);

    this.catalog = initCatalog("my-catalog", ImmutableMap.of());
    ((TestPolarisEventListener) polarisEventListener).clear();
  }

  @AfterEach
  public void after() throws IOException {
    catalog.close();
    metaStoreManager.purge(polarisContext);
  }

  protected LocalIcebergCatalog catalog() {
    return catalog;
  }

  protected LocalIcebergCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    LocalIcebergCatalog icebergCatalog = newIcebergCatalog(CATALOG_NAME);
    fileIO = new InMemoryFileIO();
    icebergCatalog.setCatalogFileIo(fileIO);
    ImmutableMap.Builder<String, String> propertiesBuilder =
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putAll(additionalProperties);
    icebergCatalog.initialize(CATALOG_NAME, propertiesBuilder.buildKeepingLast());
    return icebergCatalog;
  }

  protected LocalIcebergCatalog newIcebergCatalog(String catalogName) {
    return newIcebergCatalog(catalogName, metaStoreManager);
  }

  protected LocalIcebergCatalog newIcebergCatalog(
      String catalogName, PolarisMetaStoreManager metaStoreManager) {
    return newIcebergCatalog(catalogName, metaStoreManager, fileIOFactory);
  }

  protected LocalIcebergCatalog newIcebergCatalog(
      String catalogName, PolarisMetaStoreManager metaStoreManager, FileIOFactory fileIOFactory) {
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, authenticatedRoot, catalogName);
    TaskExecutor taskExecutor = Mockito.mock(TaskExecutor.class);
    return new LocalIcebergCatalog(
        diagServices,
        resolverFactory,
        metaStoreManager,
        polarisContext,
        passthroughView,
        authenticatedRoot,
        taskExecutor,
        storageAccessConfigProvider,
        fileIOFactory,
        polarisEventDispatcher,
        eventMetadataFactory);
  }

  @Test
  public void testParentChildLocationOverlapWithOptimizedSiblingCheck() {
    Namespace ns = Namespace.of("ns-for-overlap-test");
    catalog().createNamespace(ns);

    // Create a table at a nested location first.
    TableIdentifier childTable = TableIdentifier.of(ns, "child-table");
    String childLoc = STORAGE_LOCATION + "/overlap-parent/child";
    catalog().buildTable(childTable, SCHEMA).withLocation(childLoc).create();

    // Creating a second table at the parent (prefix) location must be rejected. This is the
    // regression case for the NoSQL hasOverlappingSiblings fix: parent-prefix overlaps have
    // shorter keys and were missed by the forward-only location-index scan.
    TableIdentifier parentTable = TableIdentifier.of(ns, "parent-table");
    String parentLoc = STORAGE_LOCATION + "/overlap-parent";
    assertThatThrownBy(
            () -> catalog().buildTable(parentTable, SCHEMA).withLocation(parentLoc).create())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Unable to create entity at location")
        .hasMessageContaining("conflicts with existing table or namespace");

    // Conversely, creating a child table after the parent table exists must also be rejected
    // (covered by the forward location-index scan).
    TableIdentifier parentTable2 = TableIdentifier.of(ns, "parent-table-2");
    String parentLoc2 = STORAGE_LOCATION + "/overlap-parent-2";
    catalog().buildTable(parentTable2, SCHEMA).withLocation(parentLoc2).create();

    TableIdentifier anotherChildTable = TableIdentifier.of(ns, "another-child-table");
    String anotherChildLoc = STORAGE_LOCATION + "/overlap-parent-2/child";
    assertThatThrownBy(
            () ->
                catalog()
                    .buildTable(anotherChildTable, SCHEMA)
                    .withLocation(anotherChildLoc)
                    .create())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Unable to create entity at location")
        .hasMessageContaining("conflicts with existing table or namespace");
  }
}
