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
package org.apache.polaris.extension.auth.opa.test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsInAnyOrder;

import io.restassured.http.ContentType;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Entity-level list filtering against a real OPA that answers through the batch rule.
 *
 * <p>The principal {@code lister-*} is denied the container-level {@code LIST_*} operations, so
 * Polaris asks OPA about every candidate. {@code OpaBatchStartupAction} sets {@code max-batch-size}
 * to 2, so the candidates are sent in several chunks. The policy allows only entities named {@code
 * visible_*}, plus one table that only the batch rule allows.
 */
public class OpaBatchListFilteringIT extends OpaIntegrationTestBase {

  private static final String FILTERING_PROPERTY =
      "polaris.config.enable-entity-level-list-filtering";

  private String catalogName;
  private String baseLocation;
  private String rootToken;
  private String listerToken;

  @BeforeEach
  void setUp(@TempDir Path tempDir) throws Exception {
    rootToken = getRootToken();
    listerToken = createPrincipalAndGetToken("lister-" + UUID.randomUUID());
    catalogName = "opa-batch-" + UUID.randomUUID().toString().replace("-", "");
    Path warehouse = tempDir.resolve("warehouse");
    Files.createDirectory(warehouse);
    baseLocation = warehouse.toUri().toString();
    createFileCatalog(
        rootToken,
        catalogName,
        baseLocation,
        List.of(baseLocation),
        Map.of(FILTERING_PROPERTY, "true"));
  }

  @Test
  void listNamespacesReturnsOnlyTheNamespacesOpaAllows() {
    for (String name : List.of("visible_a", "visible_b", "visible_c", "hidden_d", "hidden_e")) {
      createNamespace(rootToken, catalogName, name);
    }

    given()
        .header("Authorization", "Bearer " + listerToken)
        .get("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(200)
        .body(
            "namespaces",
            containsInAnyOrder(List.of("visible_a"), List.of("visible_b"), List.of("visible_c")));
  }

  @Test
  void listTablesReturnsOnlyTheTablesOpaAllows() throws Exception {
    String namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    createNamespace(rootToken, catalogName, namespace);
    for (String table :
        List.of(
            "visible_t1",
            "visible_t2",
            "visible_t3",
            "hidden_t4",
            "hidden_t5",
            "batch_only_table")) {
      registerTable(namespace, table);
    }

    // The caller is denied LIST_TABLES on the namespace, so only OPA's per-table answers count.
    // "batch_only_table" is allowed only by the batch rule, so seeing it proves the batch endpoint
    // was used. Six candidates with a batch size of 2 are sent in three chunks.
    given()
        .header("Authorization", "Bearer " + listerToken)
        .get("/api/catalog/v1/{cat}/namespaces/{ns}/tables", catalogName, namespace)
        .then()
        .statusCode(200)
        .body(
            "identifiers.name",
            containsInAnyOrder("visible_t1", "visible_t2", "visible_t3", "batch_only_table"));

    // A caller allowed LIST_TABLES on the namespace sees everything and is not filtered.
    given()
        .header("Authorization", "Bearer " + rootToken)
        .get("/api/catalog/v1/{cat}/namespaces/{ns}/tables", catalogName, namespace)
        .then()
        .statusCode(200)
        .body(
            "identifiers.name",
            containsInAnyOrder(
                "visible_t1",
                "visible_t2",
                "visible_t3",
                "hidden_t4",
                "hidden_t5",
                "batch_only_table"));
  }

  @Test
  void listViewsReturnsOnlyTheViewsOpaAllows() {
    String namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    createNamespace(rootToken, catalogName, namespace);
    for (String view : List.of("visible_v1", "visible_v2", "hidden_v3", "hidden_v4")) {
      createView(namespace, view);
    }

    given()
        .header("Authorization", "Bearer " + listerToken)
        .get("/api/catalog/v1/{cat}/namespaces/{ns}/views", catalogName, namespace)
        .then()
        .statusCode(200)
        .body("identifiers.name", containsInAnyOrder("visible_v1", "visible_v2"));
  }

  private void registerTable(String namespace, String tableName) throws Exception {
    String tableLocation =
        baseLocation + (baseLocation.endsWith("/") ? "" : "/") + namespace + "/" + tableName;
    Path metadataPath =
        Path.of(
            URI.create(
                tableLocation
                    + (tableLocation.endsWith("/") ? "" : "/")
                    + "metadata/v1.metadata.json"));
    Files.createDirectories(metadataPath.getParent());
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema, PartitionSpec.unpartitioned(), tableLocation, Map.of());
    Files.writeString(metadataPath, TableMetadataParser.toJson(metadata));

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(
            toJson(
                Map.of(
                    "name",
                    tableName,
                    "metadata-location",
                    metadataPath.toUri().toString(),
                    "stage-create",
                    false)))
        .post("/api/catalog/v1/{cat}/namespaces/{ns}/register", catalogName, namespace)
        .then()
        .statusCode(200);
  }

  private void createView(String namespace, String viewName) {
    Map<String, Object> body =
        Map.of(
            "name",
            viewName,
            "schema",
            Map.of(
                "type",
                "struct",
                "schema-id",
                0,
                "fields",
                List.of(Map.of("id", 1, "name", "id", "required", true, "type", "long"))),
            "view-version",
            Map.of(
                "version-id",
                1,
                "timestamp-ms",
                System.currentTimeMillis(),
                "schema-id",
                0,
                "summary",
                Map.of("engine-name", "spark", "engine-version", "3.5"),
                "default-namespace",
                List.of(namespace),
                "representations",
                List.of(Map.of("type", "sql", "sql", "select 1 as id", "dialect", "spark"))),
            "properties",
            Map.of());

    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(body))
        .post("/api/catalog/v1/{cat}/namespaces/{ns}/views", catalogName, namespace)
        .then()
        .statusCode(200);
  }
}
