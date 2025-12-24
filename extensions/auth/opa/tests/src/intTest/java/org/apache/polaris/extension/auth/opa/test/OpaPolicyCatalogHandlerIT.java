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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * OPA authorization coverage for catalog policy endpoints:
 *
 * <ul>
 *   <li>List/create policies
 *   <li>Attach/detach policy mappings
 *   <li>Delete policies
 * </ul>
 */
@QuarkusTest
@TestProfile(OpaIntegrationTest.StaticTokenOpaProfile.class)
public class OpaPolicyCatalogHandlerIT extends OpaIntegrationTestBase {

  private String catalogName;
  private String namespace;
  private String baseLocation;
  private String rootToken;

  @BeforeEach
  void setupBaseCatalog() throws Exception {
    rootToken = getRootToken();
    catalogName = "opa-policy-" + UUID.randomUUID().toString().replace("-", "");
    namespace = "ns_" + UUID.randomUUID().toString().replace("-", "");
    baseLocation = java.nio.file.Files.createTempDirectory("opa-policy").toUri().toString();
    createFileCatalog(rootToken, catalogName, baseLocation, List.of(baseLocation));
    createNamespace(rootToken, catalogName, namespace);
  }

  @Test
  void policyListAndAttachAuthorization() throws Exception {
    String rootToken = this.rootToken;
    String strangerToken = createPrincipalAndGetToken("stranger-" + UUID.randomUUID());
    String policyName = "pol_" + UUID.randomUUID().toString().replace("-", "");

    Map<String, Object> createPolicyRequest =
        Map.of(
            "name", policyName,
            "type", "system.data-compaction",
            "description", "opa policy test",
            "content",
                """
                {
                  "version":"2025-02-03",
                  "enable":true,
                  "config":{"target_file_size_bytes":134217728}
                }
                """);

    // Root creates policy
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(createPolicyRequest))
        .post("/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies", catalogName, namespace)
        .then()
        .statusCode(200);

    // Stranger cannot list policies
    given()
        .header("Authorization", "Bearer " + strangerToken)
        .get("/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies", catalogName, namespace)
        .then()
        .statusCode(403);

    // Root lists policies
    given()
        .header("Authorization", "Bearer " + rootToken)
        .get("/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies", catalogName, namespace)
        .then()
        .statusCode(200);

    Map<String, Object> attachRequest =
        Map.of("target", Map.of("type", "catalog", "path", List.of()), "parameters", Map.of());

    // Stranger cannot attach policy
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + strangerToken)
        .body(toJson(attachRequest))
        .put(
            "/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies/{policy}/mappings",
            catalogName,
            namespace,
            policyName)
        .then()
        .statusCode(403);

    // Root attaches policy to catalog
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(attachRequest))
        .put(
            "/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies/{policy}/mappings",
            catalogName,
            namespace,
            policyName)
        .then()
        .statusCode(204);

    // Detach policy for cleanup
    given()
        .contentType(ContentType.JSON)
        .header("Authorization", "Bearer " + rootToken)
        .body(toJson(attachRequest))
        .post(
            "/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies/{policy}/mappings",
            catalogName,
            namespace,
            policyName)
        .then()
        .statusCode(204);

    // Delete policy
    given()
        .header("Authorization", "Bearer " + rootToken)
        .delete(
            "/api/catalog/polaris/v1/{cat}/namespaces/{ns}/policies/{policy}",
            catalogName,
            namespace,
            policyName)
        .then()
        .statusCode(204);
  }
}
