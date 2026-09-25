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
package org.apache.polaris.service.http;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

/**
 * Checks over the wire that an unreadable {@code pageSize} is answered with {@code 400 Bad Request}
 * on every route that accepts it, and that the answers which were already correct did not move.
 *
 * <p>The catalog in these requests does not exist, and it does not need to: a query parameter is
 * converted before the resource method runs, so the rejection happens whether or not the rest of
 * the path resolves. The requests without {@code pageSize} show what the same path answers
 * otherwise, which is how these tests tell a rejected parameter apart from a missing catalog.
 */
@QuarkusTest
public class NumericQueryParameterTest {

  private static final String CATALOG = "nonexistent_catalog";
  private static final String ICEBERG = "/api/catalog/v1/" + CATALOG;
  private static final String POLARIS = "/api/catalog/polaris/v1/" + CATALOG;

  private static String token;

  private static RequestSpecification authenticated() {
    if (token == null) {
      token =
          given()
              .contentType(ContentType.URLENC)
              .formParam("grant_type", "client_credentials")
              .formParam("scope", "PRINCIPAL_ROLE:ALL")
              .formParam("client_id", "test-admin")
              .formParam("client_secret", "test-secret")
              .when()
              .post("/api/catalog/v1/oauth/tokens")
              .then()
              .statusCode(200)
              .extract()
              .path("access_token");
    }
    return given().header("Authorization", "Bearer " + token);
  }

  private static void expectPageSizeRejected(String path) {
    authenticated()
        .when()
        .get(path)
        .then()
        .statusCode(400)
        .body("error.message", is("pageSize must be an integer"))
        .body("error.type", is("BadRequestException"))
        .body("error.code", is(400));
  }

  private static void expectNamespaceNotFound(String path) {
    authenticated()
        .when()
        .get(path)
        .then()
        .statusCode(404)
        .body("error.message", is("Namespace does not exist: ''"))
        .body("error.type", is("NoSuchNamespaceException"))
        .body("error.code", is(404));
  }

  @Test
  public void testListNamespacesRejectsAnUnreadablePageSize() {
    expectPageSizeRejected(ICEBERG + "/namespaces?pageSize=large");
  }

  @Test
  public void testListTablesRejectsAnUnreadablePageSize() {
    expectPageSizeRejected(ICEBERG + "/namespaces/ns/tables?pageSize=large");
  }

  @Test
  public void testListViewsRejectsAnUnreadablePageSize() {
    expectPageSizeRejected(ICEBERG + "/namespaces/ns/views?pageSize=large");
  }

  @Test
  public void testListPoliciesRejectsAnUnreadablePageSize() {
    expectPageSizeRejected(POLARIS + "/namespaces/ns/policies?pageSize=large");
  }

  @Test
  public void testGetApplicablePoliciesRejectsAnUnreadablePageSize() {
    expectPageSizeRejected(POLARIS + "/applicable-policies?pageSize=large");
  }

  @Test
  public void testListGenericTablesRejectsAnUnreadablePageSize() {
    expectPageSizeRejected(POLARIS + "/namespaces/ns/generic-tables?pageSize=large");
  }

  @Test
  public void testAMissingCatalogIsStillNotFound() {
    expectNamespaceNotFound(ICEBERG + "/namespaces");
  }

  @Test
  public void testAReadablePageSizeStillReachesTheResource() {
    expectNamespaceNotFound(ICEBERG + "/namespaces?pageSize=5");
  }

  @Test
  public void testAnEmptyPageSizeStillReachesTheResource() {
    // An empty value binds as if the parameter had not been sent. The converter is not consulted.
    expectNamespaceNotFound(ICEBERG + "/namespaces?pageSize=");
  }

  @Test
  public void testAnUnknownRouteIsStillNotFound() {
    authenticated()
        .when()
        .get(ICEBERG + "/no-such-subresource")
        .then()
        .statusCode(404)
        .body("error.message", is("Unable to find matching target resource method"))
        .body("error.type", is("NotFoundException"))
        .body("error.code", is(404));
  }

  @Test
  public void testAPageSizeBelowTheMinimumIsStillRejectedByValidation() {
    // The converter reads 0 successfully and hands it on. The minimum declared on this route is
    // what refuses it, so this pins that path and the converter cannot quietly take it over.
    authenticated()
        .when()
        .get(POLARIS + "/namespaces/ns/policies?pageSize=0")
        .then()
        .statusCode(400)
        .body("error.type", is("ResteasyReactiveViolationException"))
        .body("error.code", is(400));
  }
}
