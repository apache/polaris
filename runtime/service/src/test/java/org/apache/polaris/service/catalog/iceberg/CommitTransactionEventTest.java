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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.polaris.service.events.PolarisEventType;
import org.apache.polaris.service.events.listeners.InMemoryEventCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

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

  @Test
  void testCommitTransactionCleansUpMetadataOnFailure(@TempDir Path tempDir) {
    String location = tempDir.toAbsolutePath().toUri().toString();
    if (location.endsWith("/")) {
      location = location.substring(0, location.length() - 1);
    }

    // Create TestServices with a spy that will fail on updateEntitiesPropertiesIfNotChanged
    // but only AFTER initial setup (table creation) succeeds.
    AtomicBoolean shouldFail = new AtomicBoolean(false);
    TestServices testServices =
        TestServices.builder()
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    "true",
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE")))
            .metaStoreManagerDecorator(
                msm -> {
                  org.apache.polaris.core.persistence.PolarisMetaStoreManager spy =
                      Mockito.spy(msm);
                  Mockito.doAnswer(
                          invocation -> {
                            if (shouldFail.get()) {
                              return new EntitiesResult(
                                  BaseResult.ReturnStatus.ENTITY_CANNOT_BE_RESOLVED,
                                  "simulated CAS failure");
                            }
                            return invocation.callRealMethod();
                          })
                      .when(spy)
                      .updateEntitiesPropertiesIfNotChanged(Mockito.any(), Mockito.any());
                  return spy;
                })
            .build();

    createCatalogAndNamespace(testServices, Map.of(), location);

    String table1Name = "cleanup-table-1";
    String table2Name = "cleanup-table-2";
    createTable(testServices, table1Name, location);
    createTable(testServices, table2Name, location);

    // Capture exact set of metadata file paths before the failing transaction
    Set<Path> metadataFilesBefore = metadataFiles(tempDir);

    // Now enable the CAS failure and attempt a commitTransaction
    shouldFail.set(true);
    assertThatThrownBy(
            () ->
                testServices
                    .restApi()
                    .commitTransaction(
                        catalog,
                        generateCommitTransactionRequest(false, table1Name, table2Name),
                        IDEMPOTENCY_KEY,
                        testServices.realmContext(),
                        testServices.securityContext()))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Transaction commit failed");

    // After the failed transaction, no new metadata files should remain (they were cleaned up).
    Set<Path> metadataFilesAfter = metadataFiles(tempDir);
    assertThat(metadataFilesAfter).isEqualTo(metadataFilesBefore);
  }

  private static Set<Path> metadataFiles(Path directory) {
    try (Stream<Path> files = Files.walk(directory)) {
      return files.filter(p -> p.toString().endsWith(".metadata.json")).collect(Collectors.toSet());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
