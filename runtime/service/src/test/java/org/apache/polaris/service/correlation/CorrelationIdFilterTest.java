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

package org.apache.polaris.service.correlation;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectSpy;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.smallrye.mutiny.Uni;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2Api;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestHTTPEndpoint(IcebergRestOAuth2Api.class)
@SuppressWarnings("UastIncorrectHttpHeaderInspection")
public class CorrelationIdFilterTest {

  @InjectSpy CorrelationIdGenerator correlationIdGenerator;

  @BeforeEach
  void resetMocks() {
    Mockito.reset(correlationIdGenerator);
  }

  @Test
  void testSuccessWithGeneratedRequestId() {
    givenTokenRequest()
        .when()
        .post()
        .then()
        .statusCode(200)
        .body(containsString("access_token"))
        .header("Polaris-Request-Id", anything());
    verify(correlationIdGenerator, times(1)).generateCorrelationId(any());
  }

  @Test
  void testSuccessWithCustomRequestId() {
    givenTokenRequest()
        .header("Polaris-Request-Id", "custom-request-id")
        .when()
        .post()
        .then()
        .statusCode(200)
        .body(containsString("access_token"))
        .header("Polaris-Request-Id", "custom-request-id");
    verify(correlationIdGenerator, never()).generateCorrelationId(any());
  }

  @Test
  void testError() {
    doReturn(Uni.createFrom().failure(new RuntimeException("test error")))
        .when(correlationIdGenerator)
        .generateCorrelationId(any());
    givenTokenRequest()
        .when()
        .post()
        .then()
        .statusCode(500)
        .body("error.message", is("Request ID generation failed"))
        .body("error.type", is("RequestIdGenerationError"))
        .body("error.code", is(500));
    verify(correlationIdGenerator, times(1)).generateCorrelationId(any());
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
