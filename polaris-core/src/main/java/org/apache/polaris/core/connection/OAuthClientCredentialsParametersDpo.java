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
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.OAuthClientCredentialsParameters;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.secrets.SecretReference;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * The internal persistence-object counterpart to OAuthClientCredentialsParameters defined in the
 * API model.
 */
public class OAuthClientCredentialsParametersDpo extends AuthenticationParametersDpo {

  private static final Joiner COLON_JOINER = Joiner.on(":");

  @JsonProperty(value = "tokenUri")
  private final String tokenUri;

  @JsonProperty(value = "clientId")
  private final String clientId;

  @JsonProperty(value = "clientSecretReference")
  private final SecretReference clientSecretReference;

  @JsonProperty(value = "scopes")
  private final List<String> scopes;

  public OAuthClientCredentialsParametersDpo(
      @JsonProperty(value = "tokenUri", required = false) @Nullable String tokenUri,
      @JsonProperty(value = "clientId", required = true) @Nonnull String clientId,
      @JsonProperty(value = "clientSecretReference", required = true) @Nonnull
          SecretReference clientSecretReference,
      @JsonProperty(value = "scopes", required = false) @Nullable List<String> scopes) {
    super(AuthenticationType.OAUTH.getCode());

    this.tokenUri = tokenUri;
    this.clientId = clientId;
    this.clientSecretReference = clientSecretReference;
    this.scopes = scopes;

    validateTokenUri(tokenUri);
  }

  public @Nullable String getTokenUri() {
    return tokenUri;
  }

  public @Nonnull String getClientId() {
    return clientId;
  }

  public @Nonnull SecretReference getClientSecretReference() {
    return clientSecretReference;
  }

  public @Nullable List<String> getScopes() {
    return scopes;
  }

  @JsonIgnore
  public @Nonnull String getScopesAsString() {
    return OAuth2Util.toScope(
        Objects.requireNonNullElse(scopes, List.of(OAuth2Properties.CATALOG_SCOPE)));
  }

  @JsonIgnore
  private @Nonnull String getCredentialAsConcatenatedString(UserSecretsManager secretsManager) {
    String clientSecret = secretsManager.readSecret(getClientSecretReference());
    return COLON_JOINER.join(clientId, clientSecret);
  }

  @Override
  public @Nonnull Map<String, String> asIcebergCatalogProperties(
      UserSecretsManager secretsManager, PolarisCredentialManager credentialManager) {
    HashMap<String, String> properties = new HashMap<>();
    if (getTokenUri() != null) {
      properties.put(OAuth2Properties.OAUTH2_SERVER_URI, getTokenUri());
    }
    properties.put(OAuth2Properties.CREDENTIAL, getCredentialAsConcatenatedString(secretsManager));
    properties.put(OAuth2Properties.SCOPE, getScopesAsString());
    return properties;
  }

  @Override
  public @Nonnull AuthenticationParameters asAuthenticationParametersModel() {
    return OAuthClientCredentialsParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.OAUTH)
        .setTokenUri(getTokenUri())
        .setClientId(getClientId())
        .setScopes(getScopes())
        .build();
  }

  /** Validates the token URI. */
  protected void validateTokenUri(String tokenUri) {
    if (tokenUri == null) {
      return;
    }

    try {
      URI uri = URI.create(tokenUri);
      URL url = uri.toURL();
    } catch (IllegalArgumentException | MalformedURLException e) {
      throw new IllegalArgumentException("Invalid token URI: " + tokenUri, e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("authenticationTypeCode", getAuthenticationTypeCode())
        .add("tokenUri", getTokenUri())
        .add("clientId", getClientId())
        .add("clientSecretReference", getClientSecretReference())
        .add("scopes", getScopesAsString())
        .toString();
  }
}
