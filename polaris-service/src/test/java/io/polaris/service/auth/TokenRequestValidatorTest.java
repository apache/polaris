package io.polaris.service.auth;

import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TokenRequestValidatorTest {
  @Test
  public void testValidateForClientCredentialsFlowNullClientId() {
    Assertions.assertEquals(
        OAuthTokenErrorResponse.Error.invalid_client,
        new TokenRequestValidator()
            .validateForClientCredentialsFlow(null, "notnull", "notnull", "nontnull")
            .get());
    Assertions.assertEquals(
        OAuthTokenErrorResponse.Error.invalid_client,
        new TokenRequestValidator()
            .validateForClientCredentialsFlow("", "notnull", "notnull", "nonnull")
            .get());
  }

  @Test
  public void testValidateForClientCredentialsFlowNullClientSecret() {
    Assertions.assertEquals(
        OAuthTokenErrorResponse.Error.invalid_client,
        new TokenRequestValidator()
            .validateForClientCredentialsFlow("client-id", null, "notnull", "nontnull")
            .get());
    Assertions.assertEquals(
        OAuthTokenErrorResponse.Error.invalid_client,
        new TokenRequestValidator()
            .validateForClientCredentialsFlow("client-id", "", "notnull", "notnull")
            .get());
  }

  @Test
  public void testValidateForClientCredentialsFlowInvalidGrantType() {
    Assertions.assertEquals(
        OAuthTokenErrorResponse.Error.invalid_grant,
        new TokenRequestValidator()
            .validateForClientCredentialsFlow(
                "client-id", "client-secret", "not-client-credentials", "notnull")
            .get());
    Assertions.assertEquals(
        OAuthTokenErrorResponse.Error.invalid_grant,
        new TokenRequestValidator()
            .validateForClientCredentialsFlow("client-id", "client-secret", "grant", "notnull")
            .get());
  }

  @Test
  public void testValidateForClientCredentialsFlowInvalidScope() {
    for (String scope :
        Arrays.asList("null", "", ",", "ALL", "PRINCIPAL_ROLE:", "PRINCIPAL_ROLE")) {
      Assertions.assertEquals(
          OAuthTokenErrorResponse.Error.invalid_scope,
          new TokenRequestValidator()
              .validateForClientCredentialsFlow(
                  "client-id", "client-secret", "client_credentials", scope)
              .get());
    }
  }

  @Test
  public void testValidateForClientCredentialsFlowAllValid() {
    Assertions.assertEquals(
        Optional.empty(),
        new TokenRequestValidator()
            .validateForClientCredentialsFlow(
                "client-id", "client-secret", "client_credentials", "PRINCIPAL_ROLE:ALL"));
  }
}
