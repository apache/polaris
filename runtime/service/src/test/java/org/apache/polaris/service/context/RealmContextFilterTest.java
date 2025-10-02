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

package org.apache.polaris.service.context;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectSpy;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.smallrye.common.annotation.Identifier;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2Api;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestHTTPEndpoint(IcebergRestOAuth2Api.class)
class RealmContextFilterTest {

  @InjectSpy
  @Identifier("default")
  RealmContextResolver realmContextResolver;

  @BeforeEach
  void resetMocks() {
    Mockito.reset(realmContextResolver);
  }

  @Test
  void testSuccess() {
    givenTokenRequest().when().post().then().statusCode(200).body(containsString("access_token"));
    verify(realmContextResolver, times(1)).resolveRealmContext(any(), any(), any(), anyFunction());
  }

  @Test
  void testError() {
    doReturn(CompletableFuture.failedFuture(new RuntimeException("test error")))
        .when(realmContextResolver)
        .resolveRealmContext(any(), any(), any(), anyFunction());
    givenTokenRequest()
        .when()
        .post()
        .then()
        .statusCode(404)
        .body("error.message", is("Missing or invalid realm"))
        .body("error.type", is("MissingOrInvalidRealm"))
        .body("error.code", is(404));
    verify(realmContextResolver, times(1)).resolveRealmContext(any(), any(), any(), anyFunction());
  }

  private static RequestSpecification givenTokenRequest() {
    return given()
        .contentType(ContentType.URLENC)
        .formParam("grant_type", "client_credentials")
        .formParam("scope", "PRINCIPAL_ROLE:ALL")
        .formParam("client_id", "test-admin")
        .formParam("client_secret", "test-secret");
  }

  @SuppressWarnings("unchecked")
  private static Function<String, String> anyFunction() {
    return any(Function.class);
  }
}
