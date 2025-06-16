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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.annotation.Nonnull;
import java.util.Map;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.BearerAuthenticationParameters;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.connection.iceberg.IcebergCatalogPropertiesProvider;
import org.apache.polaris.core.secrets.UserSecretReference;

/**
 * The internal persistence-object counterpart to AuthenticationParameters defined in the API model.
 * Important: JsonSubTypes must be kept in sync with {@link AuthenticationType}.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "authenticationTypeCode")
@JsonSubTypes({
  @JsonSubTypes.Type(value = OAuthClientCredentialsParametersDpo.class, name = "1"),
  @JsonSubTypes.Type(value = BearerAuthenticationParametersDpo.class, name = "2"),
  @JsonSubTypes.Type(value = ImplicitAuthenticationParametersDpo.class, name = "3"),
  @JsonSubTypes.Type(value = SigV4AuthenticationParametersDpo.class, name = "4"),
})
public abstract class AuthenticationParametersDpo implements IcebergCatalogPropertiesProvider {

  public static final String INLINE_CLIENT_SECRET_REFERENCE_KEY = "inlineClientSecretReference";
  public static final String INLINE_BEARER_TOKEN_REFERENCE_KEY = "inlineBearerTokenReference";

  @JsonProperty(value = "authenticationTypeCode")
  private final int authenticationTypeCode;

  public AuthenticationParametersDpo(
      @JsonProperty(value = "authenticationTypeCode", required = true) int authenticationTypeCode) {
    this.authenticationTypeCode = authenticationTypeCode;
  }

  public int getAuthenticationTypeCode() {
    return authenticationTypeCode;
  }

  @JsonIgnore
  public AuthenticationType getAuthenticationType() {
    return AuthenticationType.fromCode(authenticationTypeCode);
  }

  public abstract @Nonnull AuthenticationParameters asAuthenticationParametersModel();

  public static AuthenticationParametersDpo fromAuthenticationParametersModelWithSecrets(
      AuthenticationParameters authenticationParameters,
      Map<String, UserSecretReference> secretReferences) {
    final AuthenticationParametersDpo config;
    switch (authenticationParameters.getAuthenticationType()) {
      case OAUTH:
        OAuthClientCredentialsParameters oauthClientCredentialsModel =
            (OAuthClientCredentialsParameters) authenticationParameters;
        config =
            new OAuthClientCredentialsParametersDpo(
                oauthClientCredentialsModel.getTokenUri(),
                oauthClientCredentialsModel.getClientId(),
                secretReferences.get(INLINE_CLIENT_SECRET_REFERENCE_KEY),
                oauthClientCredentialsModel.getScopes());
        break;
      case BEARER:
        BearerAuthenticationParameters bearerAuthenticationParametersModel =
            (BearerAuthenticationParameters) authenticationParameters;
        config =
            new BearerAuthenticationParametersDpo(
                secretReferences.get(INLINE_BEARER_TOKEN_REFERENCE_KEY));
        break;
      case IMPLICIT:
        config = new ImplicitAuthenticationParametersDpo();
        break;
      case SIGV4:
        // SigV4 authentication is not secret-based
        SigV4AuthenticationParameters sigV4AuthenticationParametersModel =
            (SigV4AuthenticationParameters) authenticationParameters;
        config =
            new SigV4AuthenticationParametersDpo(
                sigV4AuthenticationParametersModel.getRoleArn(),
                sigV4AuthenticationParametersModel.getRoleSessionName(),
                sigV4AuthenticationParametersModel.getExternalId(),
                sigV4AuthenticationParametersModel.getSigningRegion(),
                sigV4AuthenticationParametersModel.getSigningName());
        break;
      default:
        throw new IllegalStateException(
            "Unsupported authentication type: " + authenticationParameters.getAuthenticationType());
    }
    return config;
  }
}
