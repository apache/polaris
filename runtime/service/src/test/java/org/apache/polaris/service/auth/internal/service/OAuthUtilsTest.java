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
