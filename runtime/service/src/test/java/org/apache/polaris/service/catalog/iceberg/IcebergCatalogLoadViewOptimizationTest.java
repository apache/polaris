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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisAuthorizerImpl;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.persistence.resolver.ResolverFactory;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.catalog.PolarisPassthroughResolutionView;
import org.apache.polaris.service.catalog.Profiles;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.catalog.io.StorageAccessConfigProvider;
import org.apache.polaris.service.config.ReservedProperties;
import org.apache.polaris.service.events.PolarisEventMetadataFactory;
import org.apache.polaris.service.events.listeners.PolarisEventListener;
import org.apache.polaris.service.storage.PolarisStorageIntegrationProviderImpl;
import org.apache.polaris.service.task.TaskExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;

/**
 * Unit tests for the loadView optimization fix (Issue #3061).
 *
 * <p>These tests verify that:
 *
 * <ul>
 *   <li>loadView() creates only one ViewOperations instance throughout its execution
 *   <li>FileIO is created once outside the retry loop, not on each retry attempt
 *   <li>Metadata is fetched only once during loadView()
 * </ul>
 */
@QuarkusTest
@TestProfile(IcebergCatalogLoadViewOptimizationTest.Profile.class)
public class IcebergCatalogLoadViewOptimizationTest {

  public static class Profile extends Profiles.DefaultProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("polaris.features.\"ALLOW_WILDCARD_LOCATION\"", "true")
          .put("polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"", "true")
          .build();
    }
  }

  private static final String CATALOG_NAME = "test-catalog";
  private static final Namespace NAMESPACE = Namespace.of("test_ns");
  private static final TableIdentifier VIEW_ID = TableIdentifier.of(NAMESPACE, "test_view");
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "test_table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @Inject ServiceIdentityProvider serviceIdentityProvider;
  @Inject StorageCredentialCache storageCredentialCache;
  @Inject PolarisDiagnostics diagServices;
  @Inject PolarisEventListener polarisEventListener;
  @Inject PolarisEventMetadataFactory eventMetadataFactory;
  @Inject ResolverFactory resolverFactory;
  @Inject ResolutionManifestFactory resolutionManifestFactory;
  @Inject PolarisMetaStoreManager metaStoreManager;
  @Inject UserSecretsManager userSecretsManager;
  @Inject CallContext callContext;
  @Inject RealmConfig realmConfig;
  @Inject StorageAccessConfigProvider storageAccessConfigProvider;
  @Inject FileIOFactory fileIOFactory;

  private IcebergCatalog catalog;
  private String realmName;
  private PolarisCallContext polarisContext;
  private PolarisPrincipal authenticatedRoot;

  @BeforeAll
  public static void setUpMocks() {
    PolarisStorageIntegrationProviderImpl mock =
        Mockito.mock(PolarisStorageIntegrationProviderImpl.class);
    QuarkusMock.installMockForType(mock, PolarisStorageIntegrationProviderImpl.class);
  }

  @BeforeEach
  public void before(TestInfo testInfo) {
    storageCredentialCache.invalidateAll();

    realmName =
        "realm_%s_%s"
            .formatted(
                testInfo.getTestMethod().map(Method::getName).orElse("test"), System.nanoTime());
    RealmContext realmContext = () -> realmName;
    QuarkusMock.installMockForType(realmContext, RealmContext.class);
    polarisContext = callContext.getPolarisCallContext();

    PrincipalEntity rootPrincipal =
        metaStoreManager.findRootPrincipal(polarisContext).orElseThrow();
    authenticatedRoot = PolarisPrincipal.of(rootPrincipal, Set.of());

    PolarisAuthorizer authorizer = new PolarisAuthorizerImpl(realmConfig);
    ReservedProperties reservedProperties = ReservedProperties.NONE;
    // Create a PolarisAdminService instance for managing catalogs
    PolarisAdminService adminService =
        new PolarisAdminService(
            polarisContext,
            resolutionManifestFactory,
            metaStoreManager,
            userSecretsManager,
            serviceIdentityProvider,
            authenticatedRoot,
            authorizer,
            reservedProperties);
    adminService.createCatalog(
        new CreateCatalogRequest(
            new CatalogEntity.Builder()
                .setName(CATALOG_NAME)
                .addProperty(
                    FeatureConfiguration.ALLOW_EXTERNAL_TABLE_LOCATION.catalogConfig(), "true")
                .addProperty(
                    FeatureConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(), "true")
                .setDefaultBaseLocation("file://tmp")
                .setStorageConfigurationInfo(
                    realmConfig,
                    new FileStorageConfigInfo(
                        StorageConfigInfo.StorageTypeEnum.FILE, List.of("file://", "/", "*")),
                    "file://tmp")
                .build()
                .asCatalog(serviceIdentityProvider)));

    catalog = newIcebergCatalog(CATALOG_NAME, fileIOFactory);
    catalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));
  }

  @AfterEach
  public void after() throws IOException {
    if (catalog != null) {
      catalog.close();
    }
    metaStoreManager.purge(polarisContext);
  }

  private IcebergCatalog newIcebergCatalog(String catalogName, FileIOFactory fileIOFactory) {
    PolarisPassthroughResolutionView passthroughView =
        new PolarisPassthroughResolutionView(
            resolutionManifestFactory, authenticatedRoot, catalogName);
    TaskExecutor taskExecutor = Mockito.mock(TaskExecutor.class);
    return new IcebergCatalog(
        diagServices,
        resolverFactory,
        metaStoreManager,
        polarisContext,
        passthroughView,
        authenticatedRoot,
        taskExecutor,
        storageAccessConfigProvider,
        fileIOFactory,
        polarisEventListener,
        eventMetadataFactory);
  }

  /**
   * Test that loadView() creates only one ViewOperations instance.
   *
   * <p>This verifies the fix for the bug in BaseMetastoreViewCatalog.loadView() where newViewOps()
   * was called twice - once to check if the view exists, and again when creating the BaseView.
   */
  @Test
  public void loadView_shouldCreateSingleViewOperationsInstance() {
    // Setup: Create a view first
    catalog.createNamespace(NAMESPACE);
    catalog
        .buildView(VIEW_ID)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NAMESPACE)
        .withQuery("spark", "SELECT 1")
        .create();

    // Create a spy of the catalog to track newViewOps() calls
    IcebergCatalog spyCatalog = spy(catalog);

    // Act: Load the view
    //View view = spyCatalog.loadView(VIEW_ID);

    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(2, false);
    LoadViewResponse response = catalogHandlerUtils.loadView(spyCatalog, VIEW_ID);
    ViewMetadata view = response.metadata();

    // Assert: newViewOps() should be reused (called exactly once)
    verify(spyCatalog, times(1)).newViewOps(VIEW_ID);
    assertThat(view).isNotNull();
  }

  /**
   * Test that FileIO is created only once during view loading, not on each retry attempt.
   *
   * <p>This verifies the fix where FileIO creation was moved outside the retry lambda in
   * doRefresh().
   */
  @Test
  public void loadView_shouldCreateFileIOOnlyOnce() {
    // Setup: Create a spy of FileIOFactory to track loadFileIO() calls
    FileIOFactory spyFactory = spy(fileIOFactory);
    IcebergCatalog testCatalog = newIcebergCatalog(CATALOG_NAME, spyFactory);
    testCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    testCatalog.createNamespace(NAMESPACE);
    testCatalog
        .buildView(VIEW_ID)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NAMESPACE)
        .withQuery("spark", "SELECT 1")
        .create();

    // Reset the spy to clear call history from view creation
    Mockito.reset(spyFactory);

    // Act: Load the view (triggers doRefresh -> refreshFromMetadataLocation)
    CatalogHandlerUtils catalogHandlerUtils = new CatalogHandlerUtils(2, false);
    LoadViewResponse response = catalogHandlerUtils.loadView(testCatalog, VIEW_ID);

    // Assert: loadFileIO should be called exactly once per loadView
    verify(spyFactory, times(1)).loadFileIO(any(), any(), any());
  }


  /**
   * Test that metadata file is read only once during view loading.
   *
   * <p>This uses a counting FileIO wrapper to verify that the metadata file is not read multiple
   * times.
   */
  @Test
  public void loadView_shouldReadMetadataFileOnlyOnce() {
    // Setup: Create a counting FileIO that tracks newInputFile calls for metadata files
    AtomicInteger metadataReadCount = new AtomicInteger(0);
    CountingFileIO countingFileIO = new CountingFileIO(metadataReadCount);

    FileIOFactory countingFactory =
        (storageAccessConfig, ioImplClassName, properties) -> countingFileIO;

    IcebergCatalog testCatalog = newIcebergCatalog(CATALOG_NAME, countingFactory);
    testCatalog.setCatalogFileIo(countingFileIO);
    testCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    testCatalog.createNamespace(NAMESPACE);
    testCatalog
        .buildView(VIEW_ID)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NAMESPACE)
        .withQuery("spark", "SELECT 1")
        .create();

    // Reset counter after view creation
    metadataReadCount.set(0);

    // Act: Load the view
    View view = testCatalog.loadView(VIEW_ID);

    // Access metadata through operations (simulating what viewResponse() does)
    BaseView baseView = (BaseView) view;
    baseView.operations().current();

    // Assert: Metadata file should be read exactly once
    // (one read during refresh, no additional reads when accessing cached metadata)
    assertThat(metadataReadCount.get()).isEqualTo(1);
  }

  /**
   * Test that accessing view properties after loadView doesn't trigger additional metadata fetches.
   *
   * <p>This simulates the viewResponse() flow where metadata is accessed to build the response.
   */
  @Test
  public void loadView_accessingPropertiesShouldNotTriggerAdditionalRefresh() {
    // Setup
    AtomicInteger metadataReadCount = new AtomicInteger(0);
    CountingFileIO countingFileIO = new CountingFileIO(metadataReadCount);

    FileIOFactory countingFactory =
        (storageAccessConfig, ioImplClassName, properties) -> countingFileIO;

    IcebergCatalog testCatalog = newIcebergCatalog(CATALOG_NAME, countingFactory);
    testCatalog.setCatalogFileIo(countingFileIO);
    testCatalog.initialize(
        CATALOG_NAME,
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.inmemory.InMemoryFileIO"));

    testCatalog.createNamespace(NAMESPACE);
    testCatalog
        .buildView(VIEW_ID)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NAMESPACE)
        .withQuery("spark", "SELECT 1")
        .create();

    metadataReadCount.set(0);

    // Act: Load view and access various properties (simulating response building)
    View view = testCatalog.loadView(VIEW_ID);

    // These accesses should all use cached metadata
    view.name();
    view.schema();
    view.properties();
    view.currentVersion();
    view.history();

    // Also access through operations directly
    BaseView baseView = (BaseView) view;
    ViewOperations ops = baseView.operations();
    ops.current();
    ops.current();
    ops.current();

    // Assert: Still only one metadata read despite multiple accesses
    assertThat(metadataReadCount.get()).isEqualTo(1);
  }

  /**
   * A FileIO wrapper that counts how many times newInputFile is called for metadata files. This
   * helps verify that metadata is not being read multiple times.
   */
  private static class CountingFileIO extends InMemoryFileIO {
    private final AtomicInteger metadataReadCount;

    CountingFileIO(AtomicInteger metadataReadCount) {
      this.metadataReadCount = metadataReadCount;
    }

    @Override
    public InputFile newInputFile(String path) {
      if (path.contains("metadata") && path.endsWith(".json")) {
        metadataReadCount.incrementAndGet();
      }
      return super.newInputFile(path);
    }
  }
}
