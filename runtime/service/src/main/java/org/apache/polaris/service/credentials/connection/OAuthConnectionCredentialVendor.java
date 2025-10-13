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
package org.apache.polaris.service.credentials.connection;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.OAuthClientCredentialsParametersDpo;
import org.apache.polaris.core.credentials.connection.CatalogAccessProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.credentials.CredentialVendorPriorities;

/**
 * Connection credential vendor for OAuth 2.0 Client Credentials authentication.
 *
 * <p>This vendor handles OAuth 2.0 client credentials flow by reading the client secret from the
 * secrets manager and formatting it as an OAuth credential for connecting to external catalogs.
 *
 * <p>The vendor provides only the OAuth credential (formatted as "clientId:clientSecret"). When
 * connecting to the remote catalog, Iceberg SDK will use this credential to fetch OAuth tokens
 * automatically.
 *
 * <p>This is the default implementation with {@code @Priority(CredentialVendorPriorities.DEFAULT)}.
 * Custom implementations can override this by providing a higher priority value.
 */
@RequestScoped
@AuthType(AuthenticationType.OAUTH)
@Priority(CredentialVendorPriorities.DEFAULT)
public class OAuthConnectionCredentialVendor implements ConnectionCredentialVendor {

  private static final Joiner COLON_JOINER = Joiner.on(":");

  private final UserSecretsManager secretsManager;

  @Inject
  public OAuthConnectionCredentialVendor(UserSecretsManager secretsManager) {
    this.secretsManager = secretsManager;
  }

  @Override
  public @Nonnull ConnectionCredentials getConnectionCredentials(
      @Nonnull ConnectionConfigInfoDpo connectionConfig) {

    // Validate authentication parameters type
    Preconditions.checkArgument(
        connectionConfig.getAuthenticationParameters()
            instanceof OAuthClientCredentialsParametersDpo,
        "Expected OAuthClientCredentialsParametersDpo, got: %s",
        connectionConfig.getAuthenticationParameters().getClass().getName());

    OAuthClientCredentialsParametersDpo oauthParams =
        (OAuthClientCredentialsParametersDpo) connectionConfig.getAuthenticationParameters();

    // Read the client secret from secrets manager
    String clientSecret = secretsManager.readSecret(oauthParams.getClientSecretReference());

    // Format credential as "clientId:clientSecret"
    String credential = COLON_JOINER.join(oauthParams.getClientId(), clientSecret);

    // Return the OAuth credential with expiration
    // OAuth credentials don't expire from Polaris's perspective - set expiration to Long.MAX_VALUE
    // to indicate infinite validity. If the credential expires, users need to update the catalog
    // entity to rotate the credential.
    return ConnectionCredentials.builder()
        .put(CatalogAccessProperty.OAUTH2_CREDENTIAL, credential)
        .put(CatalogAccessProperty.EXPIRES_AT_MS, String.valueOf(Long.MAX_VALUE))
        .build();
  }
}
