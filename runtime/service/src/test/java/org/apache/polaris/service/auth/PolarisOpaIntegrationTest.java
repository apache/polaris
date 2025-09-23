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
package org.apache.polaris.service.auth;

import static io.restassured.RestAssured.given;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.apache.polaris.test.commons.OpaIntegrationProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OpaIntegrationProfile.class)
public class PolarisOpaIntegrationTest {
  @Test
  void testOpaAllowsAdmin() {
    given()
        .header("X-Test-Principal", "admin")
        .when()
        .get("/api/catalog/namespaces")
        .then()
        .statusCode(200);
  }

  @Test
  void testOpaDeniesNonAdmin() {
    given()
        .header("X-Test-Principal", "bob")
        .when()
        .get("/api/catalog/namespaces")
        .then()
        .statusCode(403);
  }
}
