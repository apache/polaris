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

package org.apache.polaris.service.credentials;

import jakarta.annotation.Nonnull;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import java.util.EnumMap;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.service.credentials.connection.SupportsAuthType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link PolarisCredentialManager} responsible for retrieving credentials
 * used by Polaris to access external systems such as remote catalogs or cloud storage.
 *
 * <p>This implementation delegates to {@link ConnectionCredentialVendor} implementations selected
 * via CDI based on the authentication type. Each vendor handles the credential transformation logic
 * for a specific authentication mechanism (e.g., SigV4, OAuth).
 *
 * <p>Flow:
 *
 * <ol>
 *   <li>Selects the appropriate {@link ConnectionCredentialVendor} based on the authentication type
 *   <li>Delegates to the vendor to generate the final connection credentials (the vendor will
 *       resolve the service identity internally)
 * </ol>
 */
public class DefaultPolarisCredentialManager implements PolarisCredentialManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultPolarisCredentialManager.class);

  private final Instance<ConnectionCredentialVendor> credentialVendors;

  public DefaultPolarisCredentialManager(
      @Any Instance<ConnectionCredentialVendor> credentialVendors) {
    this.credentialVendors = credentialVendors;
  }

  @Override
  public @Nonnull EnumMap<ConnectionCredentialProperty, String> getConnectionCredentials(
      @Nonnull ServiceIdentityInfoDpo serviceIdentity,
      @Nonnull AuthenticationParametersDpo authenticationParameters) {

    // Select the appropriate vendor based on authentication type
    AuthenticationType authType = authenticationParameters.getAuthenticationType();
    Instance<ConnectionCredentialVendor> selectedVendor =
        credentialVendors.select(SupportsAuthType.Literal.of(authType));

    if (selectedVendor.isUnsatisfied()) {
      LOGGER.warn("No credential vendor found for authentication type: {}", authType);
      return new EnumMap<>(ConnectionCredentialProperty.class);
    }

    // Delegate to the vendor to generate credentials
    return selectedVendor.get().getConnectionCredentials(serviceIdentity, authenticationParameters);
  }
}
