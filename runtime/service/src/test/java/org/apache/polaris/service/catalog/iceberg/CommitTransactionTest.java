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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite.Condition;
import org.apache.polaris.core.persistence.dao.entity.ConditionalEntityWrite.Write;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.io.DefaultFileIOFactory;
import org.apache.polaris.service.catalog.io.FileIOFactory;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.apache.polaris.service.storage.aws.S3AccessConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class CommitTransactionTest {
  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";
  private static final String propertyName = "custom-property-1";

  // UUID v7
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  private String catalogLocation;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) {
    catalogLocation = tempDir.toAbsolutePath().toUri().toString();
    if (catalogLocation.endsWith("/")) {
      catalogLocation = catalogLocation.substring(0, catalogLocation.length() - 1);
    }
  }

  @Test
  void testEventsForSuccessfulTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table1Name = "test-table-1";
    String table2Name = "test-table-2";
    executeTransactionTest(false, table1Name, table2Name, testServices);

    // Verify that all (Before/After)CommitTransaction and (Before/After)UpdateTable events were
    // emitted
    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();
    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_COMMIT_TRANSACTION))
        .isNotNull();
    PolarisEvent beforeUpdateEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_UPDATE_TABLE);
    assertThat(beforeUpdateEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);

    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isNotNull();
    PolarisEvent afterUpdateEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_UPDATE_TABLE);
    assertThat(afterUpdateEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);
  }

  @Test
  void testEventsForUnSuccessfulTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table3Name = "test-table-3";
    String table4Name = "test-table-4";
    executeTransactionTest(true, table3Name, table4Name, testServices);

    // Verify that all (Before)CommitTable events were emitted
    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();

    // Verify that all BeforeCommitTransaction and BeforeUpdateTable events were emitted,
    // and that the AfterCommitTransaction and AfterUpdateTable events were not emitted
    assertThat(testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_COMMIT_TRANSACTION))
        .isNotNull();
    PolarisEvent beforeUpdateEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.BEFORE_UPDATE_TABLE);
    assertThat(beforeUpdateEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table4Name);

    assertThatThrownBy(
            () -> testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_COMMIT_TRANSACTION))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () -> testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_UPDATE_TABLE))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void commitTransactionSurfacesRetryableValidationFailureAsRetryableCommitConflict() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "retryable-conflict-table";
    createTable(testServices, tableName, catalogLocation);

    // Stands in for the RetryableValidationException addSnapshot raises under a concurrent commit.
    MetadataUpdate retryableUpdate =
        new MetadataUpdate() {
          @Override
          public void applyTo(TableMetadata.Builder metadataBuilder) {
            throw new RetryableValidationException(
                "Cannot add snapshot with sequence number 6 older than last sequence number 6");
          }

          @Override
          public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
            throw new UnsupportedOperationException();
          }
        };
    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, tableName),
                    List.of(),
                    List.of(retryableUpdate))));

    assertThatThrownBy(() ->
            testServices
                .restApi()
                .commitTransaction(
                    catalog,
                    request,
                    IDEMPOTENCY_KEY,
                    testServices.realmContext(),
                    testServices.securityContext()))
        .isInstanceOf(CommitFailedException.class)
        .isNotInstanceOf(ValidationException.class)
        .hasMessageContaining("Validation failed, please retry");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY
      })
  void commitTransactionRejectsWritePathChangeAsBadRequest(String propertyKey) {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "write-path-change-table";
    createTable(testServices, tableName, catalogLocation);

    assertThatThrownBy(() ->
            commitSingleUpdate(
                testServices,
                tableName,
                new MetadataUpdate.SetProperties(
                    Map.of(propertyKey, writePathUnder(tableName, "relocated")))))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("changes the locations of table")
        .hasMessageContaining(tableName);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY
      })
  void commitTransactionRejectsWritePathRemovalAsBadRequest(String propertyKey) {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "write-path-removal-table";
    createTable(
        testServices,
        tableName,
        catalogLocation,
        Map.of(propertyKey, writePathUnder(tableName, "custom")));

    assertThatThrownBy(() ->
            commitSingleUpdate(
                testServices, tableName, new MetadataUpdate.RemoveProperties(Set.of(propertyKey))))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("changes the locations of table")
        .hasMessageContaining(tableName);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY
      })
  void commitTransactionAcceptsWritePathSetToItsExistingValue(String propertyKey) {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "write-path-unchanged-table";
    String writePath = writePathUnder(tableName, "custom");
    createTable(testServices, tableName, catalogLocation, Map.of(propertyKey, writePath));

    assertThatCode(() ->
            commitSingleUpdate(
                testServices,
                tableName,
                new MetadataUpdate.SetProperties(Map.of(propertyKey, writePath))))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
        IcebergTableLikeEntity.USER_SPECIFIED_WRITE_METADATA_LOCATION_KEY
      })
  void commitTransactionAcceptsWritePathRemovalWhenTheTableHasNone(String propertyKey) {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "write-path-absent-table";
    createTable(testServices, tableName, catalogLocation);

    assertThatCode(() ->
            commitSingleUpdate(
                testServices, tableName, new MetadataUpdate.RemoveProperties(Set.of(propertyKey))))
        .doesNotThrowAnyException();
  }

  @Test
  void commitTransactionAcceptsAPropertyChangeThatLeavesTheWritePathsAlone() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "unrelated-property-table";
    createTable(
        testServices,
        tableName,
        catalogLocation,
        Map.of(
            IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
            writePathUnder(tableName, "custom")));

    assertThatCode(() ->
            commitSingleUpdate(
                testServices,
                tableName,
                new MetadataUpdate.SetProperties(Map.of("my.custom.property", "value"))))
        .doesNotThrowAnyException();
  }

  /**
   * A table written before Polaris 1.3.0 does not record its locations on the entity, so every
   * commit revalidates them and reaches the overlap check, whatever the update does.
   */
  @Test
  void commitTransactionRejectsATableThatDoesNotRecordItsLocations() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "unrecorded-locations-table";
    createTable(testServices, tableName, catalogLocation);
    removeRecordedLocations(testServices, tableName);

    assertThatThrownBy(() ->
            commitSingleUpdate(
                testServices,
                tableName,
                new MetadataUpdate.SetProperties(Map.of("my.custom.property", "value"))))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("changes the locations of table")
        .hasMessageContaining(tableName);
  }

  /**
   * The rejection sits inside the overlap validation, which reads {@code
   * ALLOW_TABLE_LOCATION_OVERLAP} for the catalog. When that validation is skipped anyway, the
   * transaction has nothing to reject.
   */
  @Test
  void commitTransactionAcceptsSetLocationWhenTableLocationOverlapAllowed() {
    TestServices testServices = createTestServices(Map.of("ALLOW_TABLE_LOCATION_OVERLAP", "true"));
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    String tableName = "set-location-overlap-allowed-table";
    createTable(testServices, tableName, catalogLocation);

    assertThatCode(() ->
            commitSingleUpdate(
                testServices,
                tableName,
                new MetadataUpdate.SetLocation(writePathUnder(tableName, "relocated"))))
        .doesNotThrowAnyException();
  }

  @Test
  void testLoadTableResponsesInCommitTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    String table1Name = "test-table-5";
    String table2Name = "test-table-6";
    executeTransactionTest(false, table1Name, table2Name, testServices);

    InMemoryEventCollector testPolarisEventDispatcher =
        (InMemoryEventCollector) testServices.polarisEventDispatcher();

    // Verify that AfterUpdateTable events contain LoadTableResponse objects
    PolarisEvent afterUpdateTableEvent =
        testPolarisEventDispatcher.getLatest(PolarisEventType.AFTER_UPDATE_TABLE);

    // Verify second table's LoadTableResponse
    assertThat(afterUpdateTableEvent.attributes().getRequired(EventAttributes.TABLE_NAME))
        .isEqualTo(table2Name);
    assertThat(afterUpdateTableEvent.attributes().getOptional(EventAttributes.TABLE_METADATA))
        .isPresent();
    assertThat(afterUpdateTableEvent.attributes().getOptional(EventAttributes.TABLE_METADATA))
        .isNotEmpty();
    TableMetadata metadata =
        afterUpdateTableEvent.attributes().getRequired(EventAttributes.TABLE_METADATA);
    assertThat(metadata).isNotNull();
    assertThat(metadata.properties()).containsEntry(propertyName, "value2");
  }

  /** What the atomic write of a multi-table commit received. */
  private record BatchWrite(
      PolarisCallContext callContext,
      PolarisMetaStoreManager metaStoreManager,
      ConditionalEntityWrite batch) {}

  /**
   * TestServices whose atomic write is recorded and, when {@code concurrentWriter} is set, is
   * preceded by a concurrent writer that updates every entity the commit only checks, as if it
   * committed after the requirements were validated.
   */
  private TestServices createTestServicesWithBatchHook(
      AtomicReference<BatchWrite> lastBatch, AtomicBoolean concurrentWriter) {
    return TestServices.builder()
        .config(
            Map.of(
                "ALLOW_INSECURE_STORAGE_TYPES",
                "true",
                "SUPPORTED_CATALOG_STORAGE_TYPES",
                List.of("FILE")))
        .metaStoreManagerDecorator(msm -> {
          PolarisMetaStoreManager spy = Mockito.spy(msm);
          Mockito.doAnswer(
                  invocation -> {
                    PolarisCallContext callCtx = invocation.getArgument(0);
                    ConditionalEntityWrite batch = invocation.getArgument(1);
                    lastBatch.set(new BatchWrite(callCtx, msm, batch));
                    if (concurrentWriter.get()) {
                      for (Condition condition : batch.conditions()) {
                        Condition.Unchanged unchanged = (Condition.Unchanged) condition;
                        assertThat(
                                msm.updateEntityPropertiesIfNotChanged(
                                        callCtx,
                                        unchanged.catalogPath(),
                                        new PolarisBaseEntity.Builder(
                                                (PolarisBaseEntity) unchanged.entity())
                                            .build())
                                    .isSuccess())
                            .isTrue();
                      }
                    }
                    return invocation.callRealMethod();
                  })
              .when(spy)
              .writeEntitiesIfConditionsHold(Mockito.any(), Mockito.any());
          return spy;
        })
        .build();
  }

  private LoadTableResponse loadTable(TestServices services, String tableName) {
    try (Response response =
        services
            .restApi()
            .loadTable(
                catalog,
                namespace,
                tableName,
                null,
                null,
                null,
                null,
                services.realmContext(),
                services.securityContext())) {
      return (LoadTableResponse) response.getEntity();
    }
  }

  /** Table {@code a} gets a property; table {@code b} only asserts it is unchanged. */
  private CommitTransactionRequest updateAAssertB(TestServices services, String a, String b) {
    String bUuid = loadTable(services, b).tableMetadata().uuid();
    return new CommitTransactionRequest(
        List.of(
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, a),
                List.of(),
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, b),
                List.of(new UpdateRequirement.AssertTableUUID(bUuid)),
                List.of())));
  }

  private void commit(TestServices services, CommitTransactionRequest request) {
    services
        .restApi()
        .commitTransaction(
            catalog, request, IDEMPOTENCY_KEY, services.realmContext(), services.securityContext());
  }

  /**
   * A table with requirements and no updates is checked, not written: the commit succeeds, the
   * table keeps its metadata file and its entity version.
   */
  @Test
  void testRequirementOnlyTableIsCheckedNotWritten() {
    AtomicReference<BatchWrite> lastBatch = new AtomicReference<>();
    TestServices testServices = createTestServicesWithBatchHook(lastBatch, new AtomicBoolean());
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "checked-a", catalogLocation);
    createTable(testServices, "checked-b", catalogLocation);
    String bMetadataBefore = loadTable(testServices, "checked-b").metadataLocation();

    commit(testServices, updateAAssertB(testServices, "checked-a", "checked-b"));

    BatchWrite write = lastBatch.get();
    assertThat(write.batch().writes())
        .extracting(w -> ((Write.Update) w).entity().getName())
        .containsExactly("checked-a");
    assertThat(write.batch().conditions())
        .extracting(c -> ((Condition.Unchanged) c).entity().getName())
        .containsExactly("checked-b");
    PolarisEntityCore bRead = ((Condition.Unchanged) write.batch().conditions().get(0)).entity();
    PolarisBaseEntity bAfter =
        write
            .metaStoreManager()
            .loadEntity(write.callContext(), bRead.getCatalogId(), bRead.getId(), bRead.getType())
            .getEntity();
    assertThat(bAfter.getEntityVersion()).isEqualTo(bRead.getEntityVersion());
    assertThat(loadTable(testServices, "checked-b").metadataLocation()).isEqualTo(bMetadataBefore);
    assertThat(loadTable(testServices, "checked-a").tableMetadata().properties())
        .containsEntry(propertyName, "value1");
  }

  /**
   * A concurrent commit to a table the transaction only asserted is detected atomically: the
   * transaction fails and none of its changes is applied (apache/polaris#5807).
   */
  @Test
  void testConcurrentChangeToRequirementOnlyTableFailsTheCommit() {
    AtomicBoolean concurrentWriter = new AtomicBoolean(false);
    TestServices testServices =
        createTestServicesWithBatchHook(new AtomicReference<>(), concurrentWriter);
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "raced-a", catalogLocation);
    createTable(testServices, "raced-b", catalogLocation);
    CommitTransactionRequest request = updateAAssertB(testServices, "raced-a", "raced-b");

    concurrentWriter.set(true);
    assertThatThrownBy(() -> commit(testServices, request))
        .isInstanceOf(CommitFailedException.class);
    concurrentWriter.set(false);

    assertThat(loadTable(testServices, "raced-a").tableMetadata().properties())
        .doesNotContainKey(propertyName);
  }

  /** Transactions that only assert the same table do not conflict, and do not change it. */
  @Test
  void testTransactionsThatOnlyAssertTheSameTableDoNotConflict() {
    AtomicReference<BatchWrite> lastBatch = new AtomicReference<>();
    TestServices testServices = createTestServicesWithBatchHook(lastBatch, new AtomicBoolean());
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "shared-a", catalogLocation);
    createTable(testServices, "shared-b", catalogLocation);
    createTable(testServices, "shared-c", catalogLocation);
    String cMetadataBefore = loadTable(testServices, "shared-c").metadataLocation();

    commit(testServices, updateAAssertB(testServices, "shared-a", "shared-c"));
    commit(testServices, updateAAssertB(testServices, "shared-b", "shared-c"));

    assertThat(loadTable(testServices, "shared-c").metadataLocation()).isEqualTo(cMetadataBefore);
    assertThat(loadTable(testServices, "shared-a").tableMetadata().properties())
        .containsEntry(propertyName, "value1");
    assertThat(loadTable(testServices, "shared-b").tableMetadata().properties())
        .containsEntry(propertyName, "value1");
  }

  /**
   * A FileIO factory whose FileIOs run {@code hook} once, the first time a metadata file at {@code
   * location} is opened after {@code armed} is set. Nothing else changes: files are read and
   * written by the default FileIO.
   */
  private static FileIOFactory fileIOFactoryWithHook(
      AtomicReference<String> location, AtomicBoolean armed, Runnable hook) {
    FileIOFactory delegate = new DefaultFileIOFactory(S3AccessConfig.empty());
    return (storageAccessConfig, ioImplClassName, properties) -> {
      FileIO io = delegate.loadFileIO(storageAccessConfig, ioImplClassName, properties);
      return (FileIO)
          Proxy.newProxyInstance(
              CommitTransactionTest.class.getClassLoader(),
              new Class<?>[] {FileIO.class},
              (proxy, method, args) -> {
                if ("newInputFile".equals(method.getName())
                    && args != null
                    && args[0] instanceof String path
                    && path.equals(location.get())
                    && armed.compareAndSet(true, false)) {
                  hook.run();
                }
                try {
                  return method.invoke(io, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    };
  }

  /**
   * The table a commit only asserts is committed concurrently after the commit read its metadata
   * and before it resolves the table's entity. The entity then carries the new version, so checking
   * that version alone would pass although the requirements were validated against the previous
   * metadata: the commit must detect that the entity no longer points to the validated metadata.
   */
  @Test
  void testRequirementOnlyTableCommittedBetweenItsLoadAndItsResolveFailsTheCommit() {
    AtomicReference<String> bMetadataLocation = new AtomicReference<>();
    AtomicBoolean armed = new AtomicBoolean(false);
    AtomicReference<TestServices> services = new AtomicReference<>();
    TestServices testServices =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    "true",
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE")))
            .fileIOFactorySupplier(() ->
                fileIOFactoryWithHook(
                    bMetadataLocation,
                    armed,
                    () ->
                        commit(
                            services.get(),
                            new CommitTransactionRequest(
                                List.of(
                                    UpdateTableRequest.create(
                                        TableIdentifier.of(namespace, "bound-b"),
                                        List.of(),
                                        List.of(
                                            new MetadataUpdate.SetProperties(
                                                Map.of(propertyName, "concurrent")))))))))
            .build();
    services.set(testServices);
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);
    createTable(testServices, "bound-a", catalogLocation);
    createTable(testServices, "bound-b", catalogLocation);
    CommitTransactionRequest request = updateAAssertB(testServices, "bound-a", "bound-b");
    bMetadataLocation.set(loadTable(testServices, "bound-b").metadataLocation());

    armed.set(true);
    assertThatThrownBy(() -> commit(testServices, request))
        .isInstanceOf(CommitFailedException.class);

    assertThat(armed).as("the concurrent commit ran").isFalse();
    assertThat(loadTable(testServices, "bound-b").tableMetadata().properties())
        .containsEntry(propertyName, "concurrent");
    assertThat(loadTable(testServices, "bound-a").tableMetadata().properties())
        .doesNotContainKey(propertyName);
  }

  private void createCatalogAndNamespace(
      TestServices services, Map<String, String> catalogConfig, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog))
            .putAll(catalogConfig);

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL, catalog, propertiesBuilder.build(), 0L, 0L, 1, config);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                catalog,
                createNamespaceRequest,
                IDEMPOTENCY_KEY,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private void createTable(TestServices services, String tableName, String baseLocation) {
    createTable(services, tableName, baseLocation, Map.of());
  }

  private void createTable(
      TestServices services,
      String tableName,
      String baseLocation,
      Map<String, String> properties) {
    CreateTableRequest.Builder requestBuilder =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName))
            .withSchema(SCHEMA);
    properties.forEach(requestBuilder::setProperty);
    services
        .restApi()
        .createTable(
            catalog,
            namespace,
            requestBuilder.build(),
            null,
            IDEMPOTENCY_KEY,
            services.realmContext(),
            services.securityContext());
  }

  /** Creates TestServices with event delegator enabled for event testing. */
  private TestServices createTestServices() {
    return createTestServices(Map.of());
  }

  private TestServices createTestServices(Map<String, Object> extraConfig) {
    Map<String, Object> config = new HashMap<>();
    config.put("ALLOW_INSECURE_STORAGE_TYPES", "true");
    config.put("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("FILE"));
    config.putAll(extraConfig);
    return TestServices.builder()
        .config(config)
        .withEventDelegator(true) // Enable event delegator
        .build();
  }

  /** Drops the locations recorded on the table entity, as for a table written before 1.3.0. */
  private void removeRecordedLocations(TestServices services, String tableName) {
    PolarisCallContext callContext = services.newCallContext();
    PolarisMetaStoreManager metaStoreManager = services.metaStoreManager();
    PolarisEntity catalogEntity =
        PolarisEntity.of(
            metaStoreManager
                .readEntityByName(
                    callContext,
                    null,
                    PolarisEntityType.CATALOG,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    catalog)
                .getEntity());
    PolarisEntity namespaceEntity =
        PolarisEntity.of(
            metaStoreManager
                .readEntityByName(
                    callContext,
                    List.of(catalogEntity),
                    PolarisEntityType.NAMESPACE,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    namespace)
                .getEntity());
    List<PolarisEntityCore> path = List.of(catalogEntity, namespaceEntity);
    IcebergTableLikeEntity entity =
        IcebergTableLikeEntity.of(
            metaStoreManager
                .readEntityByName(
                    callContext,
                    path,
                    PolarisEntityType.TABLE_LIKE,
                    PolarisEntitySubType.ICEBERG_TABLE,
                    tableName)
                .getEntity());
    Map<String, String> internalProperties = new HashMap<>(entity.getInternalPropertiesAsMap());
    internalProperties.remove(IcebergTableLikeEntity.LOCATION);
    assertThat(
            metaStoreManager
                .updateEntityPropertiesIfNotChanged(
                    callContext,
                    path,
                    new IcebergTableLikeEntity.Builder(entity)
                        .setInternalProperties(internalProperties)
                        .build())
                .isSuccess())
        .isTrue();
  }

  private String writePathUnder(String tableName, String suffix) {
    return String.format("%s/%s/%s/%s/%s", catalogLocation, catalog, namespace, tableName, suffix);
  }

  private void commitSingleUpdate(TestServices services, String tableName, MetadataUpdate update) {
    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, tableName), List.of(), List.of(update))));
    services
        .restApi()
        .commitTransaction(
            catalog, request, IDEMPOTENCY_KEY, services.realmContext(), services.securityContext());
  }

  /**
   * Executes a transaction test with the specified parameters.
   *
   * @param table1Name name of the first table
   * @param table2Name name of the second table
   * @param testServices TestServices object that will be operated on
   */
  private void executeTransactionTest(
      boolean shouldFail, String table1Name, String table2Name, TestServices testServices) {
    // Set up the test tables
    createTable(testServices, table1Name, catalogLocation);
    createTable(testServices, table2Name, catalogLocation);

    // Ignore any errors that occur during transaction commit
    try {
      testServices
          .restApi()
          .commitTransaction(
              catalog,
              generateCommitTransactionRequest(shouldFail, table1Name, table2Name),
              IDEMPOTENCY_KEY,
              testServices.realmContext(),
              testServices.securityContext());
    } catch (Exception ignored) {
    }
  }

  private CommitTransactionRequest generateCommitTransactionRequest(
      boolean shouldFail, String table1Name, String table2Name) {
    List<UpdateRequirement> updateRequirements;
    if (shouldFail) {
      // Schema ID does not exist, therefore call will fail
      updateRequirements = List.of(new UpdateRequirement.AssertCurrentSchemaID(-1));
    } else {
      updateRequirements = List.of();
    }
    return new CommitTransactionRequest(
        List.of(
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, table1Name),
                updateRequirements,
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, table2Name),
                updateRequirements,
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));
  }
}
