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
package org.apache.polaris.service.auth.internal;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.HttpHeaders;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.apache.polaris.service.auth.internal.broker.TokenBroker;
import org.junit.jupiter.api.Test;

/**
 * HTTP-level coverage for {@link InternalAuthenticationMechanism}: a transient failure from {@link
 * TokenBroker#verify(String)} must surface as the shared auth-time 503 contract (ErrorResponse +
 * Retry-After), not as an authentication failure.
 */
@QuarkusTest
public class InternalAuthenticationServiceUnavailableTest {

  @InjectMock TokenBroker tokenBroker;

  @Test
  void bearerAuthReturns503WhenTokenVerifyIsUnavailable() {
    when(tokenBroker.verify(anyString()))
        .thenThrow(new PolarisServiceUnavailableException(0, "Service unavailable"));

    given()
        .header("Authorization", "Bearer unused-token")
        .header("Polaris-Realm", "POLARIS")
        .when()
        .get("/api/management/v1/principal-roles")
        .then()
        .statusCode(503)
        .header(HttpHeaders.RETRY_AFTER, "0")
        .body("error.message", is("Service unavailable"))
        .body("error.type", is("PolarisServiceUnavailableException"))
        .body("error.code", is(503));
  }
}
