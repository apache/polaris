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

import com.google.common.base.Preconditions;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.BearerAuthenticationParametersDpo;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.credentials.connection.CatalogAccessProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.core.secrets.UserSecretsManager;
import org.apache.polaris.service.credentials.CredentialVendorPriorities;

/**
 * Connection credential vendor for Bearer token authentication.
 *
 * <p>This vendor handles Bearer token authentication by reading the bearer token from the secrets
 * manager and providing it for connecting to external catalogs.
 *
 * <p>The vendor provides only the bearer token. When connecting to the remote catalog, Iceberg SDK
 * will use this token as-is in HTTP Authorization headers. Bearer tokens typically have a limited
 * lifetime and should be refreshed by the user when they expire.
 *
 * <p>This is the default implementation with {@code @Priority(CredentialVendorPriorities.DEFAULT)}.
 * Custom implementations can override this by providing a higher priority value.
 */
@RequestScoped
@AuthType(AuthenticationType.BEARER)
@Priority(CredentialVendorPriorities.DEFAULT)
public class BearerConnectionCredentialVendor implements ConnectionCredentialVendor {

  private final UserSecretsManager secretsManager;

  @Inject
  public BearerConnectionCredentialVendor(UserSecretsManager secretsManager) {
    this.secretsManager = secretsManager;
  }

  @Override
  public @Nonnull ConnectionCredentials getConnectionCredentials(
      @Nonnull ConnectionConfigInfoDpo connectionConfig) {

    // Validate authentication parameters type
    Preconditions.checkArgument(
        connectionConfig.getAuthenticationParameters() instanceof BearerAuthenticationParametersDpo,
        "Expected BearerAuthenticationParametersDpo, got: %s",
        connectionConfig.getAuthenticationParameters().getClass().getName());

    BearerAuthenticationParametersDpo bearerParams =
        (BearerAuthenticationParametersDpo) connectionConfig.getAuthenticationParameters();

    // Read the bearer token from secrets manager
    String bearerToken = secretsManager.readSecret(bearerParams.getBearerTokenReference());

    // Return the bearer token with expiration
    // Bearer tokens don't expire from Polaris's perspective - set expiration to Long.MAX_VALUE
    // to indicate infinite validity. The token itself may have an expiration, but that's managed
    // by the token issuer, not Polaris.
    return ConnectionCredentials.builder()
        .put(CatalogAccessProperty.BEARER_TOKEN, bearerToken)
        .put(CatalogAccessProperty.EXPIRES_AT_MS, String.valueOf(Long.MAX_VALUE))
        .build();
  }
}
