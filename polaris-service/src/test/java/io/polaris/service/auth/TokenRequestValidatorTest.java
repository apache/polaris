/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
