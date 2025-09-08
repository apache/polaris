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

package org.apache.polaris.core.credentials;

import jakarta.annotation.Nonnull;
import java.util.EnumMap;
import org.apache.polaris.core.connection.AuthenticationParametersDpo;
import org.apache.polaris.core.credentials.connection.ConnectionCredentialProperty;
import org.apache.polaris.core.credentials.connection.PolarisConnectionCredsVendor;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.identity.dpo.ServiceIdentityInfoDpo;
import org.apache.polaris.core.identity.provider.ServiceIdentityProvider;

/**
 * PolarisCredentialManager is responsible for retrieving the credentials Polaris needs to access
 * remote services such as federated catalogs or cloud storage.
 *
 * <p>It combines service-managed identity information (e.g., an IAM user Polaris uses) with
 * user-defined authentication parameters (e.g., roleArn) to generate the credentials required for
 * authentication with external systems.
 *
 * <p>Typical flow:
 *
 * <ol>
 *   <li>Resolve the service identity using {@link ServiceIdentityProvider} to obtain a {@link
 *       ServiceIdentityCredential}.
 *   <li>Use the service identity credential together with user-provided authentication parameters
 *       to obtain temporary access credentials (e.g., via AWS STS AssumeRole).
 * </ol>
 *
 * <p>This design supports both SaaS and self-managed deployments, ensuring a clear separation
 * between user-provided configuration and Polaris-managed identity.
 */
public interface PolarisCredentialManager extends PolarisConnectionCredsVendor {
  @Override
  @Nonnull
  EnumMap<ConnectionCredentialProperty, String> getConnectionCredentials(
      ServiceIdentityInfoDpo serviceIdentity, AuthenticationParametersDpo authenticationParameters);
}
