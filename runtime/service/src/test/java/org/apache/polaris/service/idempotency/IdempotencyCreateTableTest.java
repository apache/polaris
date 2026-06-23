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
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.persistence.InMemoryIdempotencyStore;
import org.apache.polaris.service.TestServices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage of the handler-level idempotency flow (Model B) for {@code createTable},
 * driven through the REST adapter with idempotency enabled. The unit-level {@link
 * IdempotencyHandlerSupportTest} covers the support primitives; this test verifies the handler
 * wiring: a retry with the same key replays instead of conflicting, and reusing a key for a
 * different resource is rejected with 422.
 */
public class IdempotencyCreateTableTest {

  private static final String CATALOG = "test-catalog";
  private static final String NAMESPACE = "ns";
  // A valid UUIDv7 (version nibble 7).
  private static final UUID IDEMPOTENCY_KEY =
      UUID.fromString("0190f7f4-21d9-7e8b-9c8a-3c4f0a3e8b21");

  @Test
  void retryWithSameKeyReplaysInsteadOfConflict(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    String catalogLocation = tmpDir.resolve(CATALOG).toAbsolutePath().toUri().toString();
    String namespaceLocation = catalogLocation + "/" + NAMESPACE;
    createCatalog(services, catalogLocation);
    createNamespace(services, namespaceLocation);

    String tableName = "tbl_" + UUID.randomUUID();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();

    try (Response first = createTable(services, request)) {
      assertThat(first.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Same key + same resource: without idempotency this would throw AlreadyExistsException; with
    // Model B the handler replays an equivalent response from current catalog state.
    try (Response replay = createTable(services, request)) {
      assertThat(replay.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }

  @Test
  void reusingKeyForDifferentTableIsRejected(@TempDir Path tmpDir) {
    TestServices services = newServicesWithIdempotency();
    String catalogLocation = tmpDir.resolve(CATALOG).toAbsolutePath().toUri().toString();
    String namespaceLocation = catalogLocation + "/" + NAMESPACE;
    createCatalog(services, catalogLocation);
    createNamespace(services, namespaceLocation);

    CreateTableRequest first =
        CreateTableRequest.builder().withName("tbl_one").withSchema(SCHEMA).build();
    try (Response response = createTable(services, first)) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Same key bound to a different resource -> binding mismatch (PolarisExceptionMapper maps this
    // to HTTP 422).
    CreateTableRequest second =
        CreateTableRequest.builder().withName("tbl_two").withSchema(SCHEMA).build();
    assertThatThrownBy(() -> createTable(services, second))
        .isInstanceOf(IdempotencyConflictException.class);
  }

  @Test
  void replayAfterTableMovedOnReturns422(@TempDir Path tmpDir) {
    // B3: a replay reflects current catalog state, not stored response bytes. If the table has
    // moved
    // on since the key was recorded (here: dropped and recreated, producing a new metadata-file
    // location), a replay must surface 422 rather than silently returning divergent state.
    TestServices services = newServicesWithIdempotency();
    String catalogLocation = tmpDir.resolve(CATALOG).toAbsolutePath().toUri().toString();
    String namespaceLocation = catalogLocation + "/" + NAMESPACE;
    createCatalog(services, catalogLocation);
    createNamespace(services, namespaceLocation);

    String tableName = "tbl_" + UUID.randomUUID();
    CreateTableRequest request =
        CreateTableRequest.builder().withName(tableName).withSchema(SCHEMA).build();

    // Create with the key: records the original metadata-file location.
    try (Response first = createTable(services, request)) {
      assertThat(first.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Drop and recreate without the key: the table now has a different metadata-file location.
    try (Response dropped =
        services
            .restApi()
            .dropTable(
                CATALOG,
                NAMESPACE,
                tableName,
                null,
                false,
                services.realmContext(),
                services.securityContext())) {
      assertThat(dropped.getStatus()).isEqualTo(Response.Status.NO_CONTENT.getStatusCode());
    }
    try (Response recreated =
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
      assertThat(recreated.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }

    // Replay with the original key: binding matches, but the table moved on -> 422.
    assertThatThrownBy(() -> createTable(services, request))
        .isInstanceOf(UnprocessableEntityException.class);
  }

  @Test
  void keyForPreExistingTableStillConflicts(@TempDir Path tmpDir) {
    // A table created without an idempotency key is a genuine pre-existing resource. A later create
    // carrying a key fails the catalog uniqueness check; the post-conflict idempotency lookup finds
    // no matching record, so the conflict must surface as 409 rather than a false 200 replay.
    TestServices services = newServicesWithIdempotency();
    String catalogLocation = tmpDir.resolve(CATALOG).toAbsolutePath().toUri().toString();
    String namespaceLocation = catalogLocation + "/" + NAMESPACE;
    createCatalog(services, catalogLocation);
    createNamespace(services, namespaceLocation);

    String tableName = "tbl_" + UUID.randomUUID();
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

    assertThatThrownBy(() -> createTable(services, request))
        .isInstanceOf(AlreadyExistsException.class);
  }

  @Test
  void retryWithoutKeyStillConflicts(@TempDir Path tmpDir) {
    // Sanity check that idempotency only kicks in when a key is supplied: a second create of the
    // same table without a key behaves normally (conflict).
    TestServices services = newServicesWithIdempotency();
    String catalogLocation = tmpDir.resolve(CATALOG).toAbsolutePath().toUri().toString();
    String namespaceLocation = catalogLocation + "/" + NAMESPACE;
    createCatalog(services, catalogLocation);
    createNamespace(services, namespaceLocation);

    String tableName = "tbl_" + UUID.randomUUID();
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

    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .createTable(
                        CATALOG,
                        NAMESPACE,
                        request,
                        null,
                        null,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(AlreadyExistsException.class);
  }

  private static Response createTable(TestServices services, CreateTableRequest request) {
    return services
        .restApi()
        .createTable(
            CATALOG,
            NAMESPACE,
            request,
            null,
            IDEMPOTENCY_KEY,
            services.realmContext(),
            services.securityContext());
  }

  private static TestServices newServicesWithIdempotency() {
    Map<String, Object> config =
        Map.of(
            "ALLOW_INSECURE_STORAGE_TYPES",
            "true",
            "SUPPORTED_CATALOG_STORAGE_TYPES",
            List.of("FILE"),
            "ALLOW_NAMESPACE_CUSTOM_LOCATION",
            "true");
    return TestServices.builder()
        .config(config)
        .idempotencySupport(enabledIdempotencySupport())
        .build();
  }

  private static IdempotencyHandlerSupport enabledIdempotencySupport() {
    IdempotencyConfiguration configuration =
        new IdempotencyConfiguration() {
          @Override
          public boolean enabled() {
            return true;
          }

          @Override
          public String type() {
            return "in-memory";
          }

          @Override
          public Duration ttl() {
            return Duration.ofMinutes(5);
          }

          @Override
          public int concurrentReplayMaxAttempts() {
            return 5;
          }

          @Override
          public Duration concurrentReplayInitialBackoff() {
            return Duration.ofMillis(5);
          }
        };
    IdempotencyHandlerSupport support = new IdempotencyHandlerSupport();
    support.configuration = configuration;
    // Must match TestServices' realm (TestServices.TEST_REALM) so records and replays share a
    // realm.
    support.store = new InMemoryIdempotencyStore("test-realm");
    support.realmContext = () -> "test-realm";
    support.clock = Clock.systemUTC();
    return support;
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
                CATALOG,
                request,
                // No idempotency key for namespace setup; idempotency is scoped to createTable.
                null,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(Response.Status.OK.getStatusCode());
    }
  }
}
