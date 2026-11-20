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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
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
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.persistence.PersistenceCommitStateUnknownException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Tests which metadata files {@code commitTransaction} leaves behind when it fails: those written
 * for a definite failure are cleaned up, whether the failure comes from the final atomic batch
 * update or from a later table in the loop, while those written for a commit whose outcome is
 * unknown are retained.
 */
public class CommitTransactionMetadataCleanupTest {
  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";
  private static final String propertyName = "custom-property-1";

  // UUID v7
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  @Test
  void testCommitTransactionCleansUpMetadataOnFailure(@TempDir Path tempDir) throws Exception {
    String location = catalogLocation(tempDir);
    // The spy fails updateEntitiesPropertiesIfNotChanged only once the tables are created.
    AtomicBoolean shouldFail = new AtomicBoolean(false);
    TestServices testServices =
        servicesBuilder()
            .metaStoreManagerDecorator(msm -> {
              PolarisMetaStoreManager spy = Mockito.spy(msm);
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

    createCatalogAndNamespace(testServices, location);

    String table1Name = "cleanup-table-1";
    String table2Name = "cleanup-table-2";
    createTable(testServices, table1Name, location);
    createTable(testServices, table2Name, location);

    // Capture exact set of metadata file paths before the failing transaction
    Set<Path> metadataFilesBefore = metadataFiles(tempDir);

    // Now enable the CAS failure and attempt a commitTransaction
    shouldFail.set(true);
    assertThatThrownBy(() ->
            testServices
                .restApi()
                .commitTransaction(
                    catalog,
                    generateCommitTransactionRequest(table1Name, table2Name),
                    IDEMPOTENCY_KEY,
                    testServices.realmContext(),
                    testServices.securityContext()))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Transaction commit failed");

    // After the failed transaction, no new metadata files should remain (they were cleaned up).
    Set<Path> metadataFilesAfter = metadataFiles(tempDir);
    assertThat(metadataFilesAfter).isEqualTo(metadataFilesBefore);
  }

  @Test
  void testCommitTransactionCleansUpMetadataWhenALaterTableFails(@TempDir Path tempDir)
      throws Exception {
    String location = catalogLocation(tempDir);
    TestServices testServices = servicesBuilder().build();
    createCatalogAndNamespace(testServices, location);

    String table1Name = "later-failure-table-1";
    String table2Name = "later-failure-table-2";
    createTable(testServices, table1Name, location);
    createTable(testServices, table2Name, location);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);

    // The first table commits and writes its metadata file; the second carries a requirement that
    // cannot hold, so the transaction fails after that file is already on storage.
    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(new UpdateRequirement.AssertCurrentSchemaID(-1)),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));

    assertThatThrownBy(() ->
            testServices
                .restApi()
                .commitTransaction(
                    catalog,
                    request,
                    IDEMPOTENCY_KEY,
                    testServices.realmContext(),
                    testServices.securityContext()))
        .isInstanceOf(CommitFailedException.class);

    assertThat(metadataFiles(tempDir)).isEqualTo(metadataFilesBefore);
  }

  /**
   * Nothing is persisted while the per-table changes are still being applied, so a failure there
   * orphans the files already written whatever its type — including the ones the single-table
   * cleanup predicate does not recognise, such as an update naming a schema that does not exist.
   */
  @Test
  void testCommitTransactionCleansUpMetadataWhenALaterTableUpdateCannotBeApplied(
      @TempDir Path tempDir) throws Exception {
    String location = catalogLocation(tempDir);
    TestServices testServices = servicesBuilder().build();
    createCatalogAndNamespace(testServices, location);

    String table1Name = "unapplicable-update-table-1";
    String table2Name = "unapplicable-update-table-2";
    createTable(testServices, table1Name, location);
    createTable(testServices, table2Name, location);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);

    // The first table commits and writes its metadata file; the second points at a schema id that
    // does not exist, which fails while the updates are applied and before anything is persisted.
    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetCurrentSchema(999)))));

    assertThatThrownBy(() ->
            testServices
                .restApi()
                .commitTransaction(
                    catalog,
                    request,
                    IDEMPOTENCY_KEY,
                    testServices.realmContext(),
                    testServices.securityContext()))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(metadataFiles(tempDir)).isEqualTo(metadataFilesBefore);
  }

  @Test
  void testCommitTransactionRetainsMetadataWhenTheCommitOutcomeIsUnknown(@TempDir Path tempDir)
      throws Exception {
    String location = catalogLocation(tempDir);
    AtomicBoolean shouldFail = new AtomicBoolean(false);
    TestServices testServices =
        servicesBuilder()
            .metaStoreManagerDecorator(msm -> {
              PolarisMetaStoreManager spy = Mockito.spy(msm);
              Mockito.doAnswer(
                      invocation -> {
                        if (shouldFail.get()) {
                          throw new PersistenceCommitStateUnknownException(
                              "simulated unknown commit outcome", null);
                        }
                        return invocation.callRealMethod();
                      })
                  .when(spy)
                  .updateEntitiesPropertiesIfNotChanged(Mockito.any(), Mockito.any());
              return spy;
            })
            .build();
    createCatalogAndNamespace(testServices, location);

    String table1Name = "unknown-outcome-table-1";
    String table2Name = "unknown-outcome-table-2";
    createTable(testServices, table1Name, location);
    createTable(testServices, table2Name, location);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);

    shouldFail.set(true);
    assertThatThrownBy(() ->
            testServices
                .restApi()
                .commitTransaction(
                    catalog,
                    generateCommitTransactionRequest(table1Name, table2Name),
                    IDEMPOTENCY_KEY,
                    testServices.realmContext(),
                    testServices.securityContext()))
        .isInstanceOf(PersistenceCommitStateUnknownException.class);

    // The commit may already have been applied, so a file a published pointer could reference must
    // not be deleted.
    Set<Path> metadataFilesAfter = metadataFiles(tempDir);
    assertThat(metadataFilesAfter)
        .containsAll(metadataFilesBefore)
        .hasSizeGreaterThan(metadataFilesBefore.size());
  }

  @Test
  void testCommitTransactionCleansUpMetadataWhenALaterTableLocationIsRejected(@TempDir Path tempDir)
      throws Exception {
    String location = catalogLocation(tempDir);
    TestServices testServices =
        servicesBuilder(Map.of("ALLOW_CLIENT_SPECIFIED_TABLE_LOCATION", "false")).build();
    createCatalogAndNamespace(testServices, location);

    String table1Name = "rejected-location-table-1";
    String table2Name = "rejected-location-table-2";
    createTable(testServices, table1Name);
    createTable(testServices, table2Name);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);

    // The first table commits and writes its metadata file; the second specifies a write path,
    // which this catalog does not allow, so the transaction fails after that file is on storage.
    CommitTransactionRequest request =
        new CommitTransactionRequest(
            List.of(
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table1Name),
                    List.of(),
                    List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
                UpdateTableRequest.create(
                    TableIdentifier.of(namespace, table2Name),
                    List.of(),
                    List.of(
                        new MetadataUpdate.SetProperties(
                            Map.of(
                                IcebergTableLikeEntity.USER_SPECIFIED_WRITE_DATA_LOCATION_KEY,
                                location + "/elsewhere"))))));

    assertThatThrownBy(() ->
            testServices
                .restApi()
                .commitTransaction(
                    catalog,
                    request,
                    IDEMPOTENCY_KEY,
                    testServices.realmContext(),
                    testServices.securityContext()))
        .isInstanceOf(BadRequestException.class);

    assertThat(metadataFiles(tempDir)).isEqualTo(metadataFilesBefore);
  }

  private static String catalogLocation(Path tempDir) {
    String location = tempDir.toAbsolutePath().toUri().toString();
    return location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
  }

  private static TestServices.Builder servicesBuilder() {
    return servicesBuilder(Map.of());
  }

  private static TestServices.Builder servicesBuilder(Map<String, Object> extraConfig) {
    Map<String, Object> config = new HashMap<>();
    config.put("ALLOW_INSECURE_STORAGE_TYPES", "true");
    config.put("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("FILE"));
    config.putAll(extraConfig);
    return TestServices.builder().config(config);
  }

  private static Set<Path> metadataFiles(Path directory) throws Exception {
    try (Stream<Path> files = Files.walk(directory)) {
      return files.filter(p -> p.toString().endsWith(".metadata.json")).collect(Collectors.toSet());
    }
  }

  private CommitTransactionRequest generateCommitTransactionRequest(
      String table1Name, String table2Name) {
    return new CommitTransactionRequest(
        List.of(
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, table1Name),
                List.of(),
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value1")))),
            UpdateTableRequest.create(
                TableIdentifier.of(namespace, table2Name),
                List.of(),
                List.of(new MetadataUpdate.SetProperties(Map.of(propertyName, "value2"))))));
  }

  private void createCatalogAndNamespace(TestServices services, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, catalog));

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

  /** Creates a table without a client-specified location, so Polaris derives it. */
  private void createTable(TestServices services, String tableName) {
    createTable(services, tableName, null);
  }

  private void createTable(TestServices services, String tableName, String baseLocation) {
    CreateTableRequest.Builder requestBuilder =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA);
    if (baseLocation != null) {
      requestBuilder.withLocation(
          String.format("%s/%s/%s/%s", baseLocation, catalog, namespace, tableName));
    }
    CreateTableRequest createTableRequest = requestBuilder.build();
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
}
