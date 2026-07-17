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
import jakarta.annotation.Priority;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.connection.AuthenticationType;
import org.apache.polaris.core.connection.ConnectionConfigInfoDpo;
import org.apache.polaris.core.connection.GcpAuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialVendor;
import org.apache.polaris.core.credentials.connection.ConnectionCredentials;
import org.apache.polaris.service.credentials.CredentialVendorPriorities;
import org.jspecify.annotations.NonNull;

/**
 * Credential vendor for GCP-authenticated external catalogs.
 *
 * <p>No credential material is injected into catalog properties. Instead, this vendor validates
 * that Application Default Credentials are available on the Polaris server and lets Iceberg's
 * Google authentication flow acquire and refresh access tokens automatically at request time.
 */
@RequestScoped
@AuthType(AuthenticationType.GCP)
@Priority(CredentialVendorPriorities.DEFAULT)
public class GcpConnectionCredentialVendor implements ConnectionCredentialVendor {
  private final GoogleApplicationDefaultCredentialsProvider credentialsProvider;

  @Inject
  public GcpConnectionCredentialVendor(
      GoogleApplicationDefaultCredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  @Override
  public @NonNull ConnectionCredentials getConnectionCredentials(
      @NonNull ConnectionConfigInfoDpo connectionConfig) {
    Preconditions.checkArgument(
        connectionConfig.getAuthenticationParameters() instanceof GcpAuthenticationParametersDpo,
        "Expected GcpAuthenticationParametersDpo, got: %s",
        connectionConfig.getAuthenticationParameters().getClass().getName());

    credentialsProvider.validateCredentialsAvailable();
    return ConnectionCredentials.EMPTY;
  }
}
