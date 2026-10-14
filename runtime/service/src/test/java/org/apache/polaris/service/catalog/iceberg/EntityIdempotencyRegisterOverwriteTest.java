/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.service.catalog.iceberg;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.catalog.AccessDelegationMode;
import org.apache.polaris.service.idempotency.IdempotencyRequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage of entity-property (single-transaction) idempotency across a {@code
 * registerTable} that overwrites an existing table registration.
 *
 * <p>An overwrite registration replaces the internal properties of the table entity, which is where
 * the live idempotency key window lives. These tests pin the invariant that a registration
 * retaining the same Iceberg table identity (same table UUID) keeps that window alive, so retries
 * of already-committed requests keep replaying from current catalog state instead of re-applying
 * the update or surfacing a false create conflict, while a registration with a different table UUID
 * is a table replacement that does not inherit the keys of the previous identity.
 */
public class EntityIdempotencyRegisterOverwriteTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";
  private static final UUID IDEMPOTENCY_KEY =
      UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21");

  @Test
  void registerOverwriteKeepsLiveUpdateRetryKeys(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName, null);

    // A keyed update commits owner-note=older and stamps the key onto the entity.
    LoadTableResponse keyed =
        updateTableProperties(services, tableId, Map.of("owner-note", "older"), IDEMPOTENCY_KEY);
    assertThat(keyed.tableMetadata().properties()).containsEntry("owner-note", "older");

    // A newer unkeyed update commits owner-note=newer; the keyed window is carried forward by the
    // ordinary commit path.
    LoadTableResponse newer =
        updateTableProperties(services, tableId, Map.of("owner-note", "newer"), null);
    assertThat(newer.tableMetadata().properties()).containsEntry("owner-note", "newer");

    // Re-register the table with overwrite=true against its current metadata URI: the metadata
    // file, table UUID and property values are unchanged by this registration.
    String currentLocation = currentMetadataVersion(services, tableId);
    registerTable(services, tableId, currentLocation, true, null);

    // A retry of the already-committed keyed update must replay the original success from
    // current catalog state instead of re-applying the mutation (which would undo the newer
    // property value), and must not advance the metadata version.
    LoadTableResponse retry =
        updateTableProperties(services, tableId, Map.of("owner-note", "older"), IDEMPOTENCY_KEY);
    assertThat(retry.tableMetadata().properties()).containsEntry("owner-note", "newer");
    assertThat(retry.tableMetadata().metadataFileLocation()).isEqualTo(currentLocation);
  }

  @Test
  void registerOverwriteKeepsLiveCreateRetryKeys(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName, IDEMPOTENCY_KEY);

    // Re-register the identical current metadata URI with overwrite=true, then retry the
    // original create request with the same payload and key: it must replay the original
    // success rather than surface a false AlreadyExists conflict, because the key is still
    // live on the entity.
    String currentLocation = currentMetadataVersion(services, tableId);
    registerTable(services, tableId, currentLocation, true, null);

    // The same-payload retry with the same key replays the original success (HTTP 200) instead
    // of surfacing a false AlreadyExists conflict; the helper asserts the OK status.
    createTable(services, tableName, IDEMPOTENCY_KEY);
  }

  @Test
  void registerOverwriteWithDifferentTableUuidDropsRetryKeys(@TempDir Path tmpDir)
      throws Exception {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName, null);
    updateTableProperties(services, tableId, Map.of("owner-note", "older"), IDEMPOTENCY_KEY);
    updateTableProperties(services, tableId, Map.of("owner-note", "newer"), null);

    // A registration with a different Iceberg table UUID is a table replacement. Stage the
    // replacement metadata by rewriting the table's own current metadata with a fresh UUID,
    // kept in the same directory so the registration stays within the table's allowed
    // locations (the same way a replacement table gets staged before registering it).
    TableMetadata currentMetadata = currentTableMetadata(services, tableId);
    Path currentFile = Paths.get(URI.create(currentMetadata.metadataFileLocation()));
    Path replacementFile = currentFile.resolveSibling("replacement-00001.metadata.json");
    Files.writeString(
        replacementFile,
        TableMetadataParser.toJson(currentMetadata)
            .replace(currentMetadata.uuid(), UUID.randomUUID().toString()));
    registerTable(services, tableId, replacementFile.toUri().toString(), true, null);

    // The keyed retry is no longer live for the new identity, so it applies as a genuine
    // new update (advancing the replaced table state) instead of replaying.
    LoadTableResponse retry =
        updateTableProperties(services, tableId, Map.of("owner-note", "older"), IDEMPOTENCY_KEY);
    assertThat(retry.tableMetadata().properties()).containsEntry("owner-note", "older");
  }

  @Test
  void registerOverwriteAfterCurrentMetadataFileDeletedKeepsRetryKeys(@TempDir Path tmpDir)
      throws Exception {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    TableIdentifier tableId = TableIdentifier.of(Namespace.of(NAMESPACE), tableName);
    createTable(services, tableName, null);
    updateTableProperties(services, tableId, Map.of("p", "v"), IDEMPOTENCY_KEY);
    String currentLocation = currentMetadataVersion(services, tableId);

    // Simulate a lost table metadata file: copy the current metadata aside, delete the
    // current file of the entity, then repair by registering the copy with overwrite=true.
    // The registration resolves the identity of the existing table from entity internal
    // properties, so it works without reading the old (now missing) metadata file of the
    // entity, and the identity-preserving repair keeps the existing retry window.
    Path currentFile = Paths.get(URI.create(currentLocation));
    Path repairedFile = currentFile.resolveSibling("repaired-00001.metadata.json");
    Files.copy(currentFile, repairedFile, StandardCopyOption.REPLACE_EXISTING);
    Files.delete(currentFile);
    String repairedLocation = repairedFile.toUri().toString();
    registerTable(services, tableId, repairedLocation, true, null);

    // The keyed retry still replays from current catalog state, without re-applying.
    LoadTableResponse retry =
        updateTableProperties(services, tableId, Map.of("p", "v"), IDEMPOTENCY_KEY);
    assertThat(retry.tableMetadata().properties()).containsEntry("p", "v");
    assertThat(retry.tableMetadata().metadataFileLocation()).isEqualTo(repairedLocation);
  }

  private static LoadTableResponse updateTableProperties(
      TestServices services, TableIdentifier tableId, Map<String, String> properties, UUID key) {
    // Each call models a distinct HTTP request. In production every request gets a fresh
    // @RequestScoped IdempotencyRequestContext populated by IdempotencyKeyFilter; this
    // direct-call harness bypasses the filter chain, so reset the shared context and
    // populate it here to reproduce that per-request behavior.
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

  private static void registerTable(
      TestServices services,
      TableIdentifier tableId,
      String metadataLocation,
      boolean overwrite,
      UUID key) {
    IdempotencyRequestContext context = services.idempotencyRequestContext();
    context.clearPending();
    if (key != null) {
      context.setPendingKey(key);
    }
    ImmutableRegisterTableRequest request =
        ImmutableRegisterTableRequest.builder()
            .name(tableId.name())
            .metadataLocation(metadataLocation)
            .overwrite(overwrite)
            .build();
    services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .registerTable(
            tableId.namespace(),
            request,
            EnumSet.noneOf(AccessDelegationMode.class),
            Optional.empty());
  }

  private static TableMetadata currentTableMetadata(
      TestServices services, TableIdentifier tableId) {
    services.idempotencyRequestContext().clearPending();
    return services
        .catalogAdapter()
        .newHandler(services.securityContext(), CATALOG)
        .loadTable(
            tableId, "all", null, EnumSet.noneOf(AccessDelegationMode.class), Optional.empty())
        .get()
        .tableMetadata();
  }

  private static String currentMetadataVersion(TestServices services, TableIdentifier tableId) {
    return currentTableMetadata(services, tableId).metadataFileLocation();
  }

  private static void createTable(TestServices services, String tableName, UUID idempotencyKey) {
    IdempotencyRequestContext context = services.idempotencyRequestContext();
    context.clearPending();
    if (idempotencyKey != null) {
      context.setPendingKey(idempotencyKey);
    }
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
                idempotencyKey,
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
