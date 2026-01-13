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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
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
 * OPA authorization coverage for Iceberg catalog endpoints:
 *
 * <ul>
 *   <li>List namespaces
 *   <li>Register/create table
 *   <li>Drop table
 * </ul>
 */
@QuarkusTest
@TestProfile(OpaTestProfiles.StaticToken.class)
public class OpaIcebergCatalogHandlerIT extends OpaIntegrationTestBase {

  private String catalogName;
  private String namespace;
  private String baseLocation;
  private String rootToken;

  @BeforeEach
  void setupBaseCatalog(@TempDir Path tempDir) throws Exception {
    rootToken = getRootToken();
    catalogName = "opa-iceberg-" + UUID.randomUUID().toString().replace("-", "");
    namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    Path warehouse = tempDir.resolve("warehouse");
    Files.createDirectory(warehouse);
    baseLocation = warehouse.toUri().toString();
    String allowedNamespacePath =
        baseLocation + (baseLocation.endsWith("/") ? "" : "/") + namespace;
    createFileCatalog(
        rootToken,
        catalogName,
        baseLocation,
        List.of(allowedNamespacePath, allowedNamespacePath + "/"));
    // base namespace for list assertions
    createNamespace(rootToken, catalogName, namespace);
  }

  @Test
  void tableCreateAndDropAuthorization() throws Exception {
    String rootToken = this.rootToken;
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String tableName = "tbl_" + UUID.randomUUID().toString().replace("-", "");

    // Stranger cannot list namespaces for the catalog
    given()
        .header("Authorization", "Bearer " + strangerToken)
        .get("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(403);

    // Root can list namespaces
    given()
        .header("Authorization", "Bearer " + rootToken)
        .get("/api/catalog/v1/{cat}/namespaces", catalogName)
        .then()
        .statusCode(200);

    Map<String, Object> createTableRequest =
        buildCreateTableRequest(tableName, baseLocation, namespace);

    // Stranger cannot register table
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body(toJson(createTableRequest))
        .post("/api/catalog/v1/{cat}/namespaces/{ns}/register", catalogName, namespace)
        .then()
        .statusCode(403);

    // Root registers table
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(createTableRequest))
        .post("/api/catalog/v1/{cat}/namespaces/{ns}/register", catalogName, namespace)
        .then()
        .statusCode(200);

    // Root drops table
    given()
        .header("Authorization", "Bearer " + rootToken)
        .delete(
            "/api/catalog/v1/{cat}/namespaces/{ns}/tables/{tbl}", catalogName, namespace, tableName)
        .then()
        .statusCode(204);
  }

  private Map<String, Object> buildCreateTableRequest(
      String tableName, String baseLocation, String namespace) throws Exception {
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
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TableMetadata tableMetadata =
        TableMetadata.newTableMetadata(schema, spec, tableLocation, Map.of());
    String metadataJson = TableMetadataParser.toJson(tableMetadata);
    Files.writeString(metadataPath, metadataJson);

    return Map.of(
        "name",
        tableName,
        "metadata-location",
        metadataPath.toUri().toString(),
        "stage-create",
        false);
  }
}
