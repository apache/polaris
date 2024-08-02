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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TokenRequestValidatorTest {
  @Test
  public void testValidateForClientCredentialsFlowNullClientId() {
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow(null, "notnull", "notnull", "nontnull")
                .get())
        .isEqualTo(OAuthTokenErrorResponse.Error.invalid_client);
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow("", "notnull", "notnull", "nonnull")
                .get())
        .isEqualTo(OAuthTokenErrorResponse.Error.invalid_client);
  }

  @Test
  public void testValidateForClientCredentialsFlowNullClientSecret() {
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow("client-id", null, "notnull", "nontnull")
                .get())
        .isEqualTo(OAuthTokenErrorResponse.Error.invalid_client);
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow("client-id", "", "notnull", "notnull")
                .get())
        .isEqualTo(OAuthTokenErrorResponse.Error.invalid_client);
  }

  @Test
  public void testValidateForClientCredentialsFlowInvalidGrantType() {
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow(
                    "client-id", "client-secret", "not-client-credentials", "notnull")
                .get())
        .isEqualTo(OAuthTokenErrorResponse.Error.invalid_grant);
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow("client-id", "client-secret", "grant", "notnull")
                .get())
        .isEqualTo(OAuthTokenErrorResponse.Error.invalid_grant);
  }

  @Test
  public void testValidateForClientCredentialsFlowInvalidScope() {
    for (String scope :
        Arrays.asList("null", "", ",", "ALL", "PRINCIPAL_ROLE:", "PRINCIPAL_ROLE")) {
      Assertions.assertThat(
              new TokenRequestValidator()
                  .validateForClientCredentialsFlow(
                      "client-id", "client-secret", "client_credentials", scope)
                  .get())
          .isEqualTo(OAuthTokenErrorResponse.Error.invalid_scope);
    }
  }

  @Test
  public void testValidateForClientCredentialsFlowAllValid() {
    Assertions.assertThat(
            new TokenRequestValidator()
                .validateForClientCredentialsFlow(
                    "client-id", "client-secret", "client_credentials", "PRINCIPAL_ROLE:ALL"))
        .isEmpty();
  }
}
