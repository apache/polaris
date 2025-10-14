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
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.ImplicitAuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.CatalogAccessProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.service.credentials.CredentialVendorPriorities;

/**
 * Connection credential vendor for Implicit (no authentication) type.
 *
 * <p>This vendor handles implicit authentication where no credentials are required to connect to
 * external catalogs.
 *
 * <p>The vendor provides no credentials. When connecting to the remote catalog, Iceberg SDK will
 * not send any authentication headers or credentials. This is typically used for publicly
 * accessible catalogs or when authentication is handled externally.
 *
 * <p>This is the default implementation with {@code @Priority(CredentialVendorPriorities.DEFAULT)}.
 * Custom implementations can override this by providing a higher priority value.
 */
@RequestScoped
@AuthType(AuthenticationType.IMPLICIT)
@Priority(CredentialVendorPriorities.DEFAULT)
public class ImplicitConnectionCredentialVendor implements ConnectionCredentialVendor {

  @Override
  public @Nonnull ConnectionCredentials getConnectionCredentials(
      @Nonnull ConnectionConfigInfoDpo connectionConfig) {

    // Validate authentication parameters type
    Preconditions.checkArgument(
        connectionConfig.getAuthenticationParameters()
            instanceof ImplicitAuthenticationParametersDpo,
        "Expected ImplicitAuthenticationParametersDpo, got: %s",
        connectionConfig.getAuthenticationParameters().getClass().getName());

    // Return empty credentials for implicit (no authentication) type with expiration
    // Set expiration to Long.MAX_VALUE to indicate infinite validity
    return ConnectionCredentials.builder()
        .put(CatalogAccessProperty.EXPIRES_AT_MS, String.valueOf(Long.MAX_VALUE))
        .build();
  }
}
