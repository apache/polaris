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

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.credentials.PolarisCredentialManager;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.service.credentials.connection.AuthType;
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
 * <p>This bean is request-scoped and realm-aware, delegating all credential generation to
 * CDI-managed vendors.
 *
 * <p>Flow:
 *
 * <ol>
 *   <li>Selects the appropriate {@link ConnectionCredentialVendor} based on the authentication type
 *   <li>Delegates to the vendor to generate the final connection credentials (the vendor will
 *       resolve the service identity internally)
 * </ol>
 */
@RequestScoped
@Identifier("default")
public class DefaultPolarisCredentialManager implements PolarisCredentialManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DefaultPolarisCredentialManager.class);

  private final RealmContext realmContext;
  private final Instance<ConnectionCredentialVendor> credentialVendors;

  @Inject
  public DefaultPolarisCredentialManager(
      RealmContext realmContext, @Any Instance<ConnectionCredentialVendor> credentialVendors) {
    this.realmContext = realmContext;
    this.credentialVendors = credentialVendors;
  }

  public RealmContext getRealmContext() {
    return realmContext;
  }

  @Override
  public @Nonnull ConnectionCredentials getConnectionCredentials(
      @Nonnull ConnectionConfigInfoDpo connectionConfig) {

    AuthenticationType authType =
        connectionConfig.getAuthenticationParameters().getAuthenticationType();

    // Use CDI to select the appropriate vendor based on the authentication type
    ConnectionCredentialVendor selectedVendor;
    try {
      selectedVendor = credentialVendors.select(AuthType.Literal.of(authType)).get();
    } catch (jakarta.enterprise.inject.UnsatisfiedResolutionException e) {
      // No vendor registered for this authentication type
      throw new IllegalStateException(
          "No credential vendor available for authentication type: " + authType, e);
    } catch (jakarta.enterprise.inject.AmbiguousResolutionException e) {
      // Multiple vendors found - need @Priority to disambiguate
      throw new IllegalStateException(
          "Ambiguous connection credential vendor for authentication type: " + authType, e);
    }

    // Delegate credential generation to the selected vendor
    return selectedVendor.getConnectionCredentials(connectionConfig);
  }
}
