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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class CommitTransactionEventTest {
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
    assertThat(afterUpdateTableEvent.attributes().get(EventAttributes.TABLE_METADATA)).isPresent();
    assertThat(afterUpdateTableEvent.attributes().get(EventAttributes.TABLE_METADATA)).isNotEmpty();
    TableMetadata metadata =
        afterUpdateTableEvent.attributes().getRequired(EventAttributes.TABLE_METADATA);
    assertThat(metadata).isNotNull();
    assertThat(metadata.properties()).containsEntry(propertyName, "value2");
  }

  @Test
  void testStagedCreateViaCommitTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    // Table 1: empty staged-create (no data files)
    String emptyTableName = "staged-empty-table";
    String emptyTableLocation =
        String.format("%s/%s/%s/%s", catalogLocation, catalog, namespace, emptyTableName);
    UpdateTableRequest emptyTableCreate =
        buildStagedCreateRequest(TableIdentifier.of(namespace, emptyTableName), emptyTableLocation);

    // Table 2: staged-create with a snapshot (simulating stageCreate → write → commit flow).
    // The server doesn't read manifest/data files during commit — it only persists metadata.
    String dataTableName = "staged-data-table";
    String dataTableLocation =
        String.format("%s/%s/%s/%s", catalogLocation, catalog, namespace, dataTableName);
    String manifestListPath = dataTableLocation + "/metadata/snap-1-manifest-list.avro";
    long snapshotId = 1L;

    Snapshot snapshot =
        SnapshotParser.fromJson(
            String.format(
                "{\"snapshot-id\":%d,\"timestamp-ms\":%d,\"summary\":{\"operation\":\"append\"},"
                    + "\"manifest-list\":\"%s\",\"schema-id\":0}",
                snapshotId, System.currentTimeMillis(), manifestListPath));

    List<MetadataUpdate> dataTableUpdates =
        List.of(
            new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()),
            new MetadataUpdate.SetLocation(dataTableLocation),
            new MetadataUpdate.AddSchema(SCHEMA),
            new MetadataUpdate.SetCurrentSchema(0),
            new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned()),
            new MetadataUpdate.SetDefaultPartitionSpec(0),
            new MetadataUpdate.AddSortOrder(SortOrder.unsorted()),
            new MetadataUpdate.SetDefaultSortOrder(0),
            new MetadataUpdate.AddSnapshot(snapshot),
            new MetadataUpdate.SetSnapshotRef(
                SnapshotRef.MAIN_BRANCH, snapshotId, SnapshotRefType.BRANCH, null, null, null));

    UpdateTableRequest dataTableCreate =
        UpdateTableRequest.create(
            TableIdentifier.of(namespace, dataTableName),
            List.of(new UpdateRequirement.AssertTableDoesNotExist()),
            dataTableUpdates);

    // Commit both tables together
    CommitTransactionRequest req =
        new CommitTransactionRequest(List.of(emptyTableCreate, dataTableCreate));

    try (Response response =
        testServices
            .restApi()
            .commitTransaction(
                catalog,
                req,
                IDEMPOTENCY_KEY,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // Verify the empty table was created with no snapshot
    try (Response loadResponse =
        testServices
            .restApi()
            .loadTable(
                catalog,
                namespace,
                emptyTableName,
                null,
                null,
                null,
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(loadResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      LoadTableResponse loadTableResponse = (LoadTableResponse) loadResponse.getEntity();
      assertThat(loadTableResponse.tableMetadata().currentSnapshot()).isNull();
    }

    // Verify the data table was created with the snapshot visible
    try (Response loadResponse =
        testServices
            .restApi()
            .loadTable(
                catalog,
                namespace,
                dataTableName,
                null,
                null,
                null,
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(loadResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      LoadTableResponse loadTableResponse = (LoadTableResponse) loadResponse.getEntity();
      TableMetadata loadedMetadata = loadTableResponse.tableMetadata();
      assertThat(loadedMetadata.currentSnapshot()).isNotNull();
      assertThat(loadedMetadata.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
      assertThat(loadedMetadata.currentSnapshot().manifestListLocation())
          .isEqualTo(manifestListPath);
    }
  }

  @Test
  void testMixedStagedCreateAndUpdateViaCommitTransaction() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    // First, create an existing table that we'll update in the same transaction
    String existingTableName = "existing-table-for-mixed";
    createTable(testServices, existingTableName, catalogLocation);

    // Build a staged-create for a brand new table
    String newTableName = "staged-mixed-new-table";
    String newTableLocation =
        String.format("%s/%s/%s/%s", catalogLocation, catalog, namespace, newTableName);

    UpdateTableRequest stagedCreateChange =
        buildStagedCreateRequest(TableIdentifier.of(namespace, newTableName), newTableLocation);

    // Build a regular update for the existing table (set a property)
    UpdateTableRequest regularUpdate =
        UpdateTableRequest.create(
            TableIdentifier.of(namespace, existingTableName),
            List.of(),
            List.of(new MetadataUpdate.SetProperties(Map.of("key1", "value1"))));

    // Commit both in the same transaction: regular update + staged-create
    CommitTransactionRequest req =
        new CommitTransactionRequest(List.of(regularUpdate, stagedCreateChange));

    try (Response response =
        testServices
            .restApi()
            .commitTransaction(
                catalog,
                req,
                IDEMPOTENCY_KEY,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }

    // Verify the new table was created
    try (Response listResponse =
        testServices
            .restApi()
            .listTables(
                catalog,
                namespace,
                null,
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(listResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListTablesResponse tablesResponse = (ListTablesResponse) listResponse.getEntity();
      assertThat(tablesResponse.identifiers())
          .contains(TableIdentifier.of(namespace, newTableName));
    }
  }

  @Test
  void testStagedCreateFailsWhenTableAlreadyExists() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    // Create an existing table first
    String existingTableName = "already-existing-table";
    createTable(testServices, existingTableName, catalogLocation);

    // Now attempt a staged-create for the same table via commitTransaction
    String tableLocation =
        String.format("%s/%s/%s/%s-new", catalogLocation, catalog, namespace, existingTableName);

    UpdateTableRequest stagedCreateChange =
        buildStagedCreateRequest(TableIdentifier.of(namespace, existingTableName), tableLocation);

    CommitTransactionRequest req = new CommitTransactionRequest(List.of(stagedCreateChange));

    // Should fail because the table already exists
    assertThatThrownBy(
            () ->
                testServices
                    .restApi()
                    .commitTransaction(
                        catalog,
                        req,
                        IDEMPOTENCY_KEY,
                        testServices.realmContext(),
                        testServices.securityContext()))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void testStagedCreateFailsOnLocationOverlapWithExistingTable() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    // Create an existing table at a specific location
    String existingTableName = "overlap-existing-table";
    String sharedLocation =
        String.format("%s/%s/%s/%s", catalogLocation, catalog, namespace, existingTableName);
    createTable(testServices, existingTableName, catalogLocation);

    // Attempt a staged-create for a DIFFERENT table but at the SAME location
    String newTableName = "overlap-new-table";

    UpdateTableRequest stagedCreateChange =
        buildStagedCreateRequest(TableIdentifier.of(namespace, newTableName), sharedLocation);

    CommitTransactionRequest req = new CommitTransactionRequest(List.of(stagedCreateChange));

    // Should fail due to location overlap with existing table
    assertThatThrownBy(
            () ->
                testServices
                    .restApi()
                    .commitTransaction(
                        catalog,
                        req,
                        IDEMPOTENCY_KEY,
                        testServices.realmContext(),
                        testServices.securityContext()))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("conflicts with existing");
  }

  @Test
  void testRollbackOnFailedUpdate_stagedCreateNotPersisted() {
    TestServices testServices = createTestServices();
    createCatalogAndNamespace(testServices, Map.of(), catalogLocation);

    // Create an existing table that we'll attempt to update with a failing requirement
    String existingTableName = "rollback-existing-table";
    createTable(testServices, existingTableName, catalogLocation);

    // Build a staged-create for a brand new table
    String newTableName = "rollback-new-table";
    String newTableLocation =
        String.format("%s/%s/%s/%s", catalogLocation, catalog, namespace, newTableName);
    UpdateTableRequest stagedCreateChange =
        buildStagedCreateRequest(TableIdentifier.of(namespace, newTableName), newTableLocation);

    // Build a failing update for the existing table (schema ID -1 does not exist)
    UpdateTableRequest failingUpdate =
        UpdateTableRequest.create(
            TableIdentifier.of(namespace, existingTableName),
            List.of(new UpdateRequirement.AssertCurrentSchemaID(-1)),
            List.of(new MetadataUpdate.SetProperties(Map.of("key1", "value1"))));

    // Commit both: staged-create + failing update
    CommitTransactionRequest req =
        new CommitTransactionRequest(List.of(stagedCreateChange, failingUpdate));

    // The transaction should fail due to the bad schema requirement on the existing table
    assertThatThrownBy(
            () ->
                testServices
                    .restApi()
                    .commitTransaction(
                        catalog,
                        req,
                        IDEMPOTENCY_KEY,
                        testServices.realmContext(),
                        testServices.securityContext()))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("current schema");

    // Verify the staged-create was NOT persisted — the table should not exist
    try (Response listResponse =
        testServices
            .restApi()
            .listTables(
                catalog,
                namespace,
                null,
                null,
                testServices.realmContext(),
                testServices.securityContext())) {
      assertThat(listResponse.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
      ListTablesResponse tablesResponse = (ListTablesResponse) listResponse.getEntity();
      assertThat(tablesResponse.identifiers())
          .doesNotContain(TableIdentifier.of(namespace, newTableName));
    }
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
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName(tableName)
            .withLocation(String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName))
            .withSchema(SCHEMA)
            .build();
    services
        .restApi()
        .createTable(
            catalog,
            namespace,
            createTableRequest,
            null,
            IDEMPOTENCY_KEY,
            services.realmContext(),
            services.securityContext());
  }

  /** Creates TestServices with event delegator enabled for event testing. */
  private TestServices createTestServices() {
    Map<String, Object> config =
        Map.of(
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE"));
    return TestServices.builder()
        .config(config)
        .withEventDelegator(true) // Enable event delegator
        .build();
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

  private UpdateTableRequest buildStagedCreateRequest(
      TableIdentifier tableIdentifier, String location) {
    List<MetadataUpdate> createUpdates =
        List.of(
            new MetadataUpdate.AssignUUID(UUID.randomUUID().toString()),
            new MetadataUpdate.SetLocation(location),
            new MetadataUpdate.AddSchema(SCHEMA),
            new MetadataUpdate.SetCurrentSchema(0),
            new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned()),
            new MetadataUpdate.SetDefaultPartitionSpec(0),
            new MetadataUpdate.AddSortOrder(SortOrder.unsorted()),
            new MetadataUpdate.SetDefaultSortOrder(0));
    return UpdateTableRequest.create(
        tableIdentifier, List.of(new UpdateRequirement.AssertTableDoesNotExist()), createUpdates);
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
