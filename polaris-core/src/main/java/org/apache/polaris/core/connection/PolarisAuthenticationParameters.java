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
package org.apache.polaris.core.connection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.secrets.UserSecretReference;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "authenticationType", visible = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = PolarisOAuthClientCredentialsParameters.class, name = "OAUTH"),
  @JsonSubTypes.Type(value = PolarisBearerAuthenticationParameters.class, name = "BEARER"),
})
public abstract class PolarisAuthenticationParameters implements IcebergCatalogPropertiesProvider {

  public static final String INLINE_CLIENT_SECRET_REFERENCE_KEY = "inlineClientSecretReference";
  public static final String INLINE_BEARER_TOKEN_REFERENCE_KEY = "inlineBearerTokenReference";

  @JsonProperty(value = "authenticationType")
  private final AuthenticationType authenticationType;

  public PolarisAuthenticationParameters(
      @JsonProperty(value = "authenticationType", required = true) @Nonnull
          AuthenticationType authenticationType) {
    this.authenticationType = authenticationType;
  }

  public @Nonnull AuthenticationType getAuthenticationType() {
    return authenticationType;
  }

  public abstract AuthenticationParameters asAuthenticationParametersModel();

  public static PolarisAuthenticationParameters fromAuthenticationParametersModelWithSecrets(
      AuthenticationParameters authenticationParameters,
      Map<String, UserSecretReference> secretReferences) {
    PolarisAuthenticationParameters config = null;
    switch (authenticationParameters.getAuthenticationType()) {
      case OAUTH:
        OAuthClientCredentialsParameters oauthClientCredentialsModel =
            (OAuthClientCredentialsParameters) authenticationParameters;
        config =
            new PolarisOAuthClientCredentialsParameters(
                AuthenticationType.OAUTH,
                oauthClientCredentialsModel.getTokenUri(),
                oauthClientCredentialsModel.getClientId(),
                secretReferences.get(INLINE_CLIENT_SECRET_REFERENCE_KEY),
                oauthClientCredentialsModel.getScopes());
        break;
      case BEARER:
        BearerAuthenticationParameters bearerAuthenticationParametersModel =
            (BearerAuthenticationParameters) authenticationParameters;
        config =
            new PolarisBearerAuthenticationParameters(
                AuthenticationType.BEARER, secretReferences.get(INLINE_BEARER_TOKEN_REFERENCE_KEY));
        break;
      default:
        throw new IllegalStateException(
            "Unsupported authentication type: " + authenticationParameters.getAuthenticationType());
    }
    return config;
  }
}
