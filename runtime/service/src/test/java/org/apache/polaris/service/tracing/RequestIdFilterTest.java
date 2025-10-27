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

package org.apache.polaris.service.tracing;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2Api;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestHTTPEndpoint(IcebergRestOAuth2Api.class)
public class RequestIdFilterTest {

  @Test
  void testNoRequestId() {
    givenTokenRequest()
        .when()
        .post()
        .then()
        .statusCode(200)
        .body(containsString("access_token"))
        .header("x-request-id", nullValue());
  }

  @Test
  void testWithRequestId() {
    givenTokenRequest()
        .header("x-request-id", "custom-request-id")
        .when()
        .post()
        .then()
        .statusCode(200)
        .body(containsString("access_token"))
        .header("x-request-id", "custom-request-id");
  }

  private static RequestSpecification givenTokenRequest() {
    return given()
        .contentType(ContentType.URLENC)
        .formParam("grant_type", "client_credentials")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .formParam("client_id", "test-admin")
        .formParam("client_secret", "test-secret");
  }
}
