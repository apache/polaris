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

package org.apache.polaris.service.events.listeners.inmemory;

import static org.apache.polaris.persistence.relational.jdbc.models.ModelEvent.CONVERTER;
import static org.apache.polaris.service.it.env.PolarisClient.polarisClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.service.it.env.ClientPrincipal;
import org.apache.polaris.service.it.env.IntegrationTestsHelper;
import org.apache.polaris.service.it.env.PolarisApiEndpoints;
import org.apache.polaris.service.it.env.PolarisClient;
import org.apache.polaris.service.it.env.RestApi;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(InMemoryBufferEventListenerIntegrationTest.Profile.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
class InMemoryBufferEventListenerIntegrationTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.persistence.type", "relational-jdbc")
          .put("polaris.persistence.auto-bootstrap-types", "relational-jdbc")
          .put("quarkus.datasource.db-kind", "h2")
          .put("quarkus.otel.sdk.disabled", "false")
          .put(
              "quarkus.datasource.jdbc.url",
              "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
          .put("polaris.event-listener.type", "persistence-in-memory-buffer")
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "100ms")
          .put("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true")
          .put("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\",\"S3\"]")
          .put("polaris.features.\"ALLOW_OVERLAPPING_CATALOG_URLS\"", "true")
          .put("polaris.readiness.ignore-severe-issues", "true")
          .build();
    }
  }

  private RestApi managementApi;
  private PolarisApiEndpoints endpoints;
  private PolarisClient client;
  private String realm;
  private String authToken;
  private URI baseLocation;

  @Inject Instance<DataSource> dataSource;

  @BeforeAll
  public void setup(
      PolarisApiEndpoints apiEndpoints, ClientPrincipal adminCredentials, @TempDir Path tempDir) {
    endpoints = apiEndpoints;
    client = polarisClient(endpoints);
    realm = endpoints.realmId();
    authToken = client.obtainToken(adminCredentials.credentials());
    managementApi = client.managementApi(authToken);
    baseLocation = IntegrationTestsHelper.getTemporaryDirectory(tempDir).resolve(realm + "/");
  }

  /**
   * Reset the database state before each test to ensure test isolation. The H2 in-memory database
   * with DB_CLOSE_DELAY=-1 persists state across tests, so we need to clean up catalog-related
   * entities while preserving the realm and principal entities set up in @BeforeAll.
   */
  @BeforeEach
  public void resetDatabaseState() {
    if (dataSource.isResolvable()) {
      try (Connection conn = dataSource.get().getConnection();
          Statement stmt = conn.createStatement()) {
        // Set the schema first
        stmt.execute("SET SCHEMA POLARIS_SCHEMA");
        // Only delete events - catalogs use unique names and locations so they don't conflict
        stmt.execute("DELETE FROM EVENTS");
      } catch (Exception e) {
        // Ignore errors - tables may not exist yet on first run
      }
    }
  }

  @Test
  void testCreateCatalogAndTable() throws IOException {

    String catalogName = client.newEntityName("testCreateCatalogAndTable");
    // Use a unique base location for this catalog to avoid overlap with other catalogs
    URI catalogBaseLocation = baseLocation.resolve(catalogName + "/");

    Catalog catalog =
        PolarisCatalog.builder()
            .setName(catalogName)
            .setType(Catalog.TypeEnum.INTERNAL)
            .setProperties(CatalogProperties.builder("file:///tmp/").build())
            .setStorageConfigInfo(
                FileStorageConfigInfo.builder()
                    .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
                    .setAllowedLocations(List.of(catalogBaseLocation.toString()))
                    .build())
            .build();

    try (Response response =
        managementApi
            .request("v1/catalogs")
            .header("X-Request-ID", "12345")
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    try (RESTSessionCatalog sessionCatalog = new RESTSessionCatalog()) {

      sessionCatalog.initialize(
          "polaris_catalog_test",
          ImmutableMap.<String, String>builder()
              .put("uri", endpoints.catalogApiEndpoint().toString())
              .put(OAuth2Properties.TOKEN, authToken)
              .put("warehouse", catalogName)
              .putAll(endpoints.extraHeaders("header."))
              .put("header.X-Request-ID", "456789")
              .build());

      SessionCatalog.SessionContext sessionContext = SessionCatalog.SessionContext.createEmpty();
      Namespace ns = Namespace.of("db1");
      sessionCatalog.createNamespace(sessionContext, ns);

      sessionCatalog
          .buildTable(
              sessionContext,
              TableIdentifier.of(ns, "t1"),
              new Schema(
                  List.of(Types.NestedField.required(1, "theField", Types.StringType.get()))))
          .withSortOrder(SortOrder.unsorted())
          .withPartitionSpec(PartitionSpec.unpartitioned())
          .create();
    }

    String query =
        "SELECT * FROM polaris_schema.events WHERE realm_id = '"
            + realm
            + "' ORDER BY timestamp_ms";

    List<PolarisEvent> events =
        await()
            .atMost(Duration.ofSeconds(10))
            .until(
                () -> {
                  ImmutableList.Builder<PolarisEvent> e = ImmutableList.builder();
                  try (Connection connection = dataSource.get().getConnection();
                      Statement statement = connection.createStatement();
                      ResultSet rs = statement.executeQuery(query)) {
                    while (rs.next()) {
                      PolarisEvent event = CONVERTER.fromResultSet(rs);
                      e.add(event);
                    }
                  }
                  return e.build();
                },
                e -> e.size() >= 2);

    // FIXME: check before events when they get persisted

    PolarisEvent e1 = events.getFirst();
    assertThat(e1.getCatalogId()).isEqualTo(catalogName);
    assertThat(e1.getResourceType()).isEqualTo(PolarisEvent.ResourceType.CATALOG);
    assertThat(e1.getResourceIdentifier()).isEqualTo(catalogName);
    assertThat(e1.getEventType()).isEqualTo("AFTER_CREATE_CATALOG");
    assertThat(e1.getPrincipalName()).isEqualTo("root");
    assertThat(e1.getRequestId()).isEqualTo("12345");
    assertThat(e1.getAdditionalPropertiesAsMap())
        .containsEntry("otel.trace_flags", "01")
        .containsEntry("otel.sampled", "true")
        .hasEntrySatisfying("otel.trace_id", value -> assertThat(value).matches("[0-9a-f]{32}"))
        .hasEntrySatisfying("otel.span_id", value -> assertThat(value).matches("[0-9a-f]{16}"));

    PolarisEvent e2 = events.getLast();
    assertThat(e2.getCatalogId()).isEqualTo(catalogName);
    assertThat(e2.getResourceType()).isEqualTo(PolarisEvent.ResourceType.TABLE);
    assertThat(e2.getResourceIdentifier()).isEqualTo("db1.t1");
    assertThat(e2.getEventType()).isEqualTo("AFTER_CREATE_TABLE");
    assertThat(e2.getPrincipalName()).isEqualTo("root");
    assertThat(e2.getRequestId()).isEqualTo("456789");
    assertThat(e2.getAdditionalPropertiesAsMap())
        .containsKey("table-uuid")
        .containsEntry("otel.trace_flags", "01")
        .containsEntry("otel.sampled", "true")
        .hasEntrySatisfying("otel.trace_id", value -> assertThat(value).matches("[0-9a-f]{32}"))
        .hasEntrySatisfying("otel.span_id", value -> assertThat(value).matches("[0-9a-f]{16}"));
  }
}
