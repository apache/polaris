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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * OPA authorization coverage for generic table endpoints:
 *
 * <ul>
 *   <li>List generic tables
 *   <li>Create generic table
 *   <li>Drop generic table
 * </ul>
 */
@QuarkusTest
@TestProfile(OpaTestProfiles.StaticToken.class)
public class OpaGenericTableHandlerIT extends OpaIntegrationTestBase {

  private String catalogName;
  private String namespace;
  private String baseLocation;
  private String rootToken;

  @BeforeEach
  void setupBaseCatalog(@TempDir Path tempDir) throws Exception {
    rootToken = getRootToken();
    catalogName = "opa-gt-" + UUID.randomUUID().toString().replace("-", "");
    namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    Path warehouse = tempDir.resolve("warehouse");
    Files.createDirectory(warehouse);
    baseLocation = warehouse.toUri().toString();
    createFileCatalog(rootToken, catalogName, baseLocation, List.of(baseLocation));
    createNamespace(rootToken, catalogName, namespace);
  }

  @Test
  void genericTableCreateAndDropAuthorization() throws Exception {
    String rootToken = this.rootToken;
    String tableName = "gt_" + UUID.randomUUID().toString().replace("-", "");

    Map<String, Object> tablePayload =
        Map.of("name", tableName, "format", "ICEBERG", "doc", "doc", "properties", Map.of());

    // Root lists generic tables (initially empty)
    given()
        .header("Authorization", "Bearer " + rootToken)
        .get("/api/catalog/polaris/v1/{cat}/namespaces/{ns}/generic-tables", catalogName, namespace)
        .then()
        .statusCode(200);

    // Root creates generic table
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(tablePayload))
        .post(
            "/api/catalog/polaris/v1/{cat}/namespaces/{ns}/generic-tables", catalogName, namespace)
        .then()
        .statusCode(200);

    // Root drops generic table
    given()
        .header("Authorization", "Bearer " + rootToken)
        .delete(
            "/api/catalog/polaris/v1/{cat}/namespaces/{ns}/generic-tables/{table}",
            catalogName,
            namespace,
            tableName)
        .then()
        .statusCode(204);
  }
}
