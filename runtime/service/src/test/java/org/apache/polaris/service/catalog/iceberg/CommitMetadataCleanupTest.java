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
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.persistence.PersistenceCommitStateUnknownException;
import org.apache.polaris.core.persistence.PersistenceWriteNotStartedException;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests that single-table commit cleanup of newly written metadata files only runs for known
 * failures, not when the metastore write may already have succeeded (ambiguous outcome).
 */
public class CommitMetadataCleanupTest {
  private static final String NAMESPACE = "ns";
  private static final String CATALOG = "test-catalog";
  private static final String PROPERTY_NAME = "custom-property-1";
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  @Test
  void retainsMetadataWhenPersistenceThrowsAfterSuccessfulWrite(@TempDir Path tempDir)
      throws Exception {
    String location = catalogBaseLocation(tempDir);
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
                  PolarisMetaStoreManager spy = Mockito.spy(msm);
                  Mockito.doAnswer(
                          invocation -> {
                            Object result = invocation.callRealMethod();
                            if (shouldFail.get()) {
                              // Simulate: DB applied the CAS, then client lost the ack.
                              throw new RuntimeException(
                                  "boom", new SQLException("Connection reset", "08006"));
                            }
                            return result;
                          })
                      .when(spy)
                      .updateEntityPropertiesIfNotChanged(
                          Mockito.any(), Mockito.any(), Mockito.any());
                  return spy;
                })
            .build();

    createCatalogAndNamespace(testServices, location);
    String tableName = "ambiguous-cleanup-table";
    createTable(testServices, tableName, location);
    TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);
    shouldFail.set(true);

    assertThatThrownBy(() -> updateTable(testServices, tableId, "value-after"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("boom");

    Set<Path> metadataFilesAfter = metadataFiles(tempDir);
    assertThat(metadataFilesAfter)
        .as("New metadata must be retained when commit outcome may be success")
        .hasSizeGreaterThan(metadataFilesBefore.size());
    assertThat(metadataFilesAfter).containsAll(metadataFilesBefore);

    // Catalog pointer was applied before the thrown error; load must still resolve.
    shouldFail.set(false);
    String metadataLocation =
        testServices
            .catalogAdapter()
            .newHandler(testServices.securityContext(), CATALOG)
            .loadTable(
                tableId, "all", null, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty())
            .get()
            .tableMetadata()
            .metadataFileLocation();
    assertThat(Path.of(java.net.URI.create(metadataLocation).getPath())).exists();
  }

  @Test
  void cleansUpMetadataOnKnownConcurrentModificationFailure(@TempDir Path tempDir)
      throws Exception {
    String location = catalogBaseLocation(tempDir);
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
                  PolarisMetaStoreManager spy = Mockito.spy(msm);
                  Mockito.doAnswer(
                          invocation -> {
                            if (shouldFail.get()) {
                              return new EntityResult(
                                  BaseResult.ReturnStatus.TARGET_ENTITY_CONCURRENTLY_MODIFIED,
                                  "simulated concurrent modification");
                            }
                            return invocation.callRealMethod();
                          })
                      .when(spy)
                      .updateEntityPropertiesIfNotChanged(
                          Mockito.any(), Mockito.any(), Mockito.any());
                  return spy;
                })
            .build();

    createCatalogAndNamespace(testServices, location);
    String tableName = "known-failure-cleanup-table";
    createTable(testServices, tableName, location);
    TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);
    shouldFail.set(true);

    assertThatThrownBy(() -> updateTable(testServices, tableId, "value-conflict"))
        .isInstanceOf(CommitConflictException.class);

    Set<Path> metadataFilesAfter = metadataFiles(tempDir);
    assertThat(metadataFilesAfter)
        .as("Orphan metadata from a known CAS failure should be cleaned up")
        .isEqualTo(metadataFilesBefore);
  }

  @Test
  void cleansUpMetadataWhenWriteNeverStarted(@TempDir Path tempDir) throws Exception {
    String location = catalogBaseLocation(tempDir);
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
                  PolarisMetaStoreManager spy = Mockito.spy(msm);
                  Mockito.doAnswer(
                          invocation -> {
                            if (shouldFail.get()) {
                              // Simulate: the DB connection could not be acquired, so the write
                              // never started (a definite non-write).
                              throw new PersistenceWriteNotStartedException(
                                  "write did not start",
                                  new SQLException("Connection refused", "08001"));
                            }
                            return invocation.callRealMethod();
                          })
                      .when(spy)
                      .updateEntityPropertiesIfNotChanged(
                          Mockito.any(), Mockito.any(), Mockito.any());
                  return spy;
                })
            .build();

    createCatalogAndNamespace(testServices, location);
    String tableName = "write-not-started-cleanup-table";
    createTable(testServices, tableName, location);
    TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);

    Set<Path> metadataFilesBefore = metadataFiles(tempDir);
    shouldFail.set(true);

    assertThatThrownBy(() -> updateTable(testServices, tableId, "value-not-started"))
        .isInstanceOf(PersistenceWriteNotStartedException.class);

    Set<Path> metadataFilesAfter = metadataFiles(tempDir);
    assertThat(metadataFilesAfter)
        .as("Orphan metadata from a definite non-write should be cleaned up")
        .isEqualTo(metadataFilesBefore);
  }

  @ParameterizedTest
  @MethodSource("cleanupDecisionCases")
  void shouldCleanupMetadataOnCommitFailure(Throwable failure, boolean expectedCleanup) {
    assertThat(LocalIcebergCatalog.shouldCleanupMetadataOnCommitFailure(failure))
        .isEqualTo(expectedCleanup);
  }

  static Stream<Arguments> cleanupDecisionCases() {
    return Stream.of(
        Arguments.of(null, false),
        Arguments.of(new RuntimeException("opaque boom"), false),
        Arguments.of(
            new RuntimeException("boom", new SQLException("Connection reset", "08006")), false),
        Arguments.of(
            new PersistenceCommitStateUnknownException(
                "unknown", new SQLException("timeout", "57014")),
            false),
        // A write that never started (connection could not be acquired) is a definite non-write:
        // the freshly written metadata is a safe-to-delete orphan.
        Arguments.of(
            new PersistenceWriteNotStartedException(
                "write did not start", new SQLException("Connection refused", "08001")),
            true),
        Arguments.of(
            new org.apache.iceberg.exceptions.CommitStateUnknownException(
                new RuntimeException("db timeout")),
            false),
        Arguments.of(new CommitFailedException("concurrent metadata location"), true),
        Arguments.of(new CommitConflictException("concurrent entity version"), true),
        Arguments.of(new AlreadyExistsException("table exists"), true),
        Arguments.of(new NotFoundException("missing"), true),
        Arguments.of(new ValidationException("invalid"), true));
  }

  private static String catalogBaseLocation(Path tempDir) {
    String location = tempDir.toAbsolutePath().toUri().toString();
    if (location.endsWith("/")) {
      location = location.substring(0, location.length() - 1);
    }
    return location;
  }

  private static Set<Path> metadataFiles(Path directory) throws Exception {
    try (Stream<Path> files = Files.walk(directory)) {
      return files.filter(p -> p.toString().endsWith(".metadata.json")).collect(Collectors.toSet());
    }
  }

  private void updateTable(TestServices services, TableIdentifier tableId, String propertyValue) {
    UpdateTableRequest request =
        UpdateTableRequest.create(
            tableId,
            List.of(),
            List.of(new MetadataUpdate.SetProperties(Map.of(PROPERTY_NAME, propertyValue))));
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .updateTable(tableId, request);
  }

  private void createCatalogAndNamespace(TestServices services, String catalogLocation) {
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, CATALOG));

    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL, CATALOG, propertiesBuilder.build(), 0L, 0L, 1, config);
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
        CreateNamespaceRequest.builder().withNamespace(Namespace.of(NAMESPACE)).build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG,
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
            .withLocation(String.format("%s/%s/%s/%s", baseLocation, CATALOG, NAMESPACE, tableName))
            .withSchema(SCHEMA)
            .build();
    services
        .restApi()
        .createTable(
            CATALOG,
            NAMESPACE,
            createTableRequest,
            null,
            IDEMPOTENCY_KEY,
            services.realmContext(),
            services.securityContext());
  }
}
