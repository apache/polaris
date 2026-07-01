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
package org.apache.polaris.service.auth.internal.service;

import jakarta.ws.rs.core.Response;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

class OAuthUtilsTest {

  @Test
  void testUnauthorizedClientResponse() {
    Response response = OAuthUtils.getResponseFromError(OAuthError.unauthorized_client);

    Assertions.assertThat(response.getStatus())
        .isEqualTo(Response.Status.UNAUTHORIZED.getStatusCode());

    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(OAuthError.unauthorized_client.name(), OAuthTokenErrorResponse::getError);
  }

  @Test
  void testInvalidClientResponse() {
    assertBadRequest(OAuthError.invalid_client);
  }

  @Test
  void testInvalidGrantResponse() {
    assertBadRequest(OAuthError.invalid_grant);
  }

  @Test
  void testUnsupportedGrantTypeResponse() {
    assertBadRequest(OAuthError.unsupported_grant_type);
  }

  @Test
  void testInvalidScopeResponse() {
    assertBadRequest(OAuthError.invalid_scope);
  }

  @Test
  void testInvalidRequestUsesDefaultBranch() {
    Response response = OAuthUtils.getResponseFromError(OAuthError.invalid_request);

    Assertions.assertThat(response.getStatus())
        .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());

    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(OAuthError.invalid_request.name(), OAuthTokenErrorResponse::getError);
  }

  private void assertBadRequest(OAuthError error) {
    Response response = OAuthUtils.getResponseFromError(error);

    Assertions.assertThat(response.getStatus())
        .isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());

    Assertions.assertThat(response.getEntity())
        .isInstanceOf(OAuthTokenErrorResponse.class)
        .asInstanceOf(InstanceOfAssertFactories.type(OAuthTokenErrorResponse.class))
        .returns(error.name(), OAuthTokenErrorResponse::getError);
  }
}
