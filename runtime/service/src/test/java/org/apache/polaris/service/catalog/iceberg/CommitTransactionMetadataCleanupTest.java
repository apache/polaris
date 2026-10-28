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
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntitiesResult;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Tests that {@code commitTransaction} cleans up newly written metadata files when the final atomic
 * batch update against the metastore fails.
 */
public class CommitTransactionMetadataCleanupTest {
  private static final String namespace = "ns";
  private static final String catalog = "test-catalog";
  private static final String propertyName = "custom-property-1";

  // UUID v7
  private static final UUID IDEMPOTENCY_KEY = new UUID(116617318654508422L, -7820829973016961092L);

  @Test
  void testCommitTransactionCleansUpMetadataOnFailure(@TempDir Path tempDir) throws Exception {
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
    assertThatThrownBy(
            () ->
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
}
