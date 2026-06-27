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
package org.apache.polaris.service.idempotency;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.ws.rs.core.Response;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage of the entity-property (single-transaction) idempotency prototype for {@code
 * createTable}, driven through the REST adapter with idempotency enabled via configuration.
 *
 * <p>Unlike the decoupled-store model, the key here is embedded into the created table entity's
 * internal properties and committed atomically with the table, so a retry replays the original
 * success purely from current catalog state.
 */
public class EntityIdempotencyCreateTableTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";
  private static final UUID IDEMPOTENCY_KEY =
      UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21");

  @Test
  void retryWithSameKeyReplaysInsteadOfConflict(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();

    try (Response first = createTable(services, request, IDEMPOTENCY_KEY)) {
      assertThat(first.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Same key + same resource: without idempotency this would throw AlreadyExistsException; the
    // entity-property model replays from current catalog state because the key was committed onto
    // the table entity.
    try (Response replay = createTable(services, request, IDEMPOTENCY_KEY)) {
      assertThat(replay.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @Test
  void retryWithoutKeyStillConflicts(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();

    try (Response response = createTable(services, request, null)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // No key supplied: idempotency does not kick in, so the second create conflicts as usual.
    assertThatThrownBy(() -> createTable(services, request, null))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void keyForPreExistingTableStillConflicts(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();

    // Table created without a key: a genuine pre-existing resource carrying no idempotency key.
    try (Response response = createTable(services, request, null)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // A later create with a key finds no matching key on the existing entity -> real 409 conflict,
    // not a false replay.
    assertThatThrownBy(() -> createTable(services, request, IDEMPOTENCY_KEY))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void differentKeyForExistingTableConflicts(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    setUpCatalogAndNamespace(services, tmpDir);

    String tableName = "tbl_" + UUID.randomUUID();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();

    try (Response response = createTable(services, request, IDEMPOTENCY_KEY)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // A different key targeting an already-created table is a genuine conflict.
    UUID otherKey = UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b22");
    assertThatThrownBy(() -> createTable(services, request, otherKey))
        .isInstanceOf(AlreadyExistsException.class);
  }

  private static Response createTable(
      TestServices services, CreateTableRequest request, UUID idempotencyKey) {
    return services
        .restApi()
        .createTable(
            CATALOG,
            NAMESPACE,
            request,
            null,
            idempotencyKey,
            services.realmContext(),
            services.securityContext());
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
