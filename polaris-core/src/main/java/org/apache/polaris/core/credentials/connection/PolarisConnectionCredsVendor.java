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

package org.apache.polaris.core.credentials.connection;

import jakarta.annotation.Nonnull;
import java.util.EnumMap;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;

/**
 * Generates credentials Polaris uses to connect to external catalog services such as AWS Glue or
 * other federated endpoints. Implementations combine service-managed identity metadata (such as an
 * IAM user or role, defined in {@link ServiceIdentityInfoDpo}) with user-provided authentication
 * parameters (such as a role ARN or external ID, defined in {@link AuthenticationParametersDpo}) to
 * construct a credential map consumable by Polaris.
 *
 * <p>This interface allows pluggable behavior for different authentication mechanisms and service
 * vendors. Implementations can support service-specific credential provisioning or caching
 * strategies as needed.
 */
public interface PolarisConnectionCredsVendor {

  /**
   * Retrieve credential values required to authenticate a remote connection.
   *
   * <p>The returned credentials are derived using the combination of Polaris-managed service
   * identity and user-specified connection authentication parameters. Implementations may look up
   * the service identity credential from a secret store or local config, and use it in conjunction
   * with user-supplied data to produce scoped credentials for accessing remote services.
   *
   * @param serviceIdentity Polaris-managed identity metadata, including a reference to the backing
   *     credential (e.g., a secret ARN or ID)
   * @param authenticationParameters Authentication configuration supplied by the Polaris user
   * @return A map from {@link ConnectionCredentialProperty} to the resolved credential value, used
   *     by downstream systems to establish the connection
   */
  @Nonnull
  EnumMap<ConnectionCredentialProperty, String> getConnectionCredentials(
      ServiceIdentityInfoDpo serviceIdentity, AuthenticationParametersDpo authenticationParameters);
}
