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

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage of entity-property (single-transaction) idempotency for {@code updateTable}.
 *
 * <p>The idempotency key is stamped onto the table entity's internal properties atomically with the
 * metadata change, so a retry carrying the same key replays the original success from current
 * catalog state instead of re-applying the update (which could fail the request's requirements
 * against the already-advanced table).
 */
public class EntityIdempotencyUpdateTableTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";
  private static final UUID IDEMPOTENCY_KEY =
      UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21");

  @Test
  void retryWithSameKeyReplaysWithoutReapplying(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName);
    String versionBeforeUpdate = currentMetadataVersion(services, tableId);

    // First keyed update applies the change, stamps the key onto the entity, and advances the table
    // to a new metadata version.
    LoadTableResponse first =
        updateTableProperties(services, tableId, Map.of("p", "v"), IDEMPOTENCY_KEY);
    String versionAfterFirst = first.tableMetadata().metadataFileLocation();
    assertThat(first.tableMetadata().properties()).containsEntry("p", "v");
    assertThat(versionAfterFirst).isNotEqualTo(versionBeforeUpdate);

    // Spec-legal retry: same key AND same payload. The request replays the earlier success instead
    // of committing a second time, so the table version (metadata file) does not advance.
    LoadTableResponse replay =
        updateTableProperties(services, tableId, Map.of("p", "v"), IDEMPOTENCY_KEY);
    assertThat(replay.tableMetadata().metadataFileLocation()).isEqualTo(versionAfterFirst);
    assertThat(replay.tableMetadata().properties()).containsEntry("p", "v");
  }

  @Test
  void updateWithoutKeyAppliesEachAttempt(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName);

    // No key supplied: idempotency does not kick in, so each update applies as usual.
    LoadTableResponse first = updateTableProperties(services, tableId, Map.of("p", "first"), null);
    assertThat(first.tableMetadata().properties()).containsEntry("p", "first");

    LoadTableResponse second =
        updateTableProperties(services, tableId, Map.of("p", "second"), null);
    assertThat(second.tableMetadata().properties()).containsEntry("p", "second");
  }

  @Test
  void differentKeyIsNotReplayed(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName);

    LoadTableResponse first =
        updateTableProperties(services, tableId, Map.of("p", "first"), IDEMPOTENCY_KEY);
    assertThat(first.tableMetadata().properties()).containsEntry("p", "first");

    // A different key is not live on the entity, so this is a genuine new update, not a replay.
    UUID otherKey = UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b22");
    LoadTableResponse other =
        updateTableProperties(services, tableId, Map.of("p", "second"), otherKey);
    assertThat(other.tableMetadata().properties()).containsEntry("p", "second");
  }

  private static LoadTableResponse updateTableProperties(
      TestServices services, TableIdentifier tableId, Map<String, String> properties, UUID key) {
    // Each call models a distinct HTTP request. In production every request gets a fresh
    // @RequestScoped IdempotencyRequestContext populated by IdempotencyKeyFilter; this direct-call
    // harness bypasses the filter chain, so reset the shared context and populate it here.
    IdempotencyRequestContext context = services.idempotencyRequestContext();
    context.clearPending();
    if (key != null) {
      context.setPendingKey(key);
    }
    UpdateTableRequest request =
        UpdateTableRequest.create(
            tableId, List.of(), List.of(new MetadataUpdate.SetProperties(properties)));
    return services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .updateTable(tableId, request);
  }

  private static String currentMetadataVersion(TestServices services, TableIdentifier tableId) {
    services.idempotencyRequestContext().clearPending();
    return services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .loadTable(tableId, "all")
        .tableMetadata()
        .metadataFileLocation();
  }

  private static void createTable(TestServices services, String tableName) {
    services.idempotencyRequestContext().clearPending();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();
    try (Response response =
        services
            .restApi()
            .createTable(
                CATALOG,
                NAMESPACE,
                request,
                null,
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  private static TestServices newServicesWithIdempotency() {
    Map<String, Object> config = new HashMap<>();
    config.put("ALLOW_INSECURE_STORAGE_TYPES", "true");
    config.put("SUPPORTED_CATALOG_STORAGE_TYPES", List.of("FILE"));
    config.put("ALLOW_NAMESPACE_CUSTOM_LOCATION", "true");
    config.put("polaris.idempotency.enabled", "true");
    return TestServices.builder().config(config).build();
  }

  private static void setUpCatalogAndNamespace(TestServices services, Path tmpDir) {
    String catalogLocation = tmpDir.resolve(CATALOG).toAbsolutePath().toUri().toString();
    createCatalog(services, catalogLocation);
    createNamespace(services, catalogLocation + "/" + NAMESPACE);
  }

  private static void createCatalog(TestServices services, String catalogLocation) {
    StorageConfigInfo storageConfig =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of(catalogLocation))
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            CATALOG,
            CatalogProperties.builder()
                .setDefaultBaseLocation(String.format("%s/%s", catalogLocation, CATALOG))
                .build(),
            1725487592064L,
            1725487592064L,
            1,
            storageConfig);
    try (Response response =
        services
            .catalogsApi()
            .createCatalog(
                new CreateCatalogRequest(catalogObject),
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.CREATED.getStatusCode());
    }
  }

  private static void createNamespace(TestServices services, String location) {
    Map<String, String> properties = new HashMap<>();
    properties.put("location", location);
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(NAMESPACE))
            .setProperties(properties)
            .build();
    try (Response response =
        services
            .restApi()
            .createNamespace(
                CATALOG, request, null, services.realmContext(), services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }
}
