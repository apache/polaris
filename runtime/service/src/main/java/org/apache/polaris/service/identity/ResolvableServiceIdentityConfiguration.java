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

package org.apache.polaris.service.identity;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.polaris.core.admin.model.ServiceIdentityInfo;
import org.apache.polaris.core.identity.ServiceIdentityType;
import org.apache.polaris.core.identity.credential.ServiceIdentityCredential;
import org.apache.polaris.core.secrets.SecretReference;

/**
 * Represents a service identity configuration that can be converted into a fully initialized {@link
 * ServiceIdentityCredential}.
 *
 * <p>This interface allows identity configurations (e.g., AWS IAM) to encapsulate the logic
 * required to construct runtime credentials and metadata needed to authenticate as a
 * Polaris-managed service identity.
 */
public interface ResolvableServiceIdentityConfiguration {
  /**
   * Returns the type of service identity represented by this configuration.
   *
   * @return the service identity type, or {@link ServiceIdentityType#NULL_TYPE} if not specified
   */
  default ServiceIdentityType getType() {
    return ServiceIdentityType.NULL_TYPE;
  }

  /**
   * Converts this configuration into a {@link ServiceIdentityInfo} model containing identity
   * metadata without credentials.
   *
   * <p>This method is used when only identity information (e.g., IAM ARN) is needed for API
   * responses, without exposing sensitive credentials.
   *
   * @return an optional service identity info model, or empty if required configuration is missing
   */
  default Optional<? extends ServiceIdentityInfo> asServiceIdentityInfoModel() {
    return Optional.empty();
  }

  /**
   * Converts this configuration into a {@link ServiceIdentityCredential} with actual credentials.
   *
   * <p>This method should only be called when credentials are actually needed for authentication.
   * Implementations should construct the appropriate credential object (e.g., {@link
   * org.apache.polaris.core.identity.credential.AwsIamServiceIdentityCredential}) using the
   * configured values and the provided secret reference.
   *
   * @param secretReference the secret reference to associate with this credential for persistence
   * @return an optional service identity credential, or empty if required configuration is missing
   */
  default Optional<? extends ServiceIdentityCredential> asServiceIdentityCredential(
      @Nonnull SecretReference secretReference) {
    return Optional.empty();
  }

  /**
   * Returns the default resolvable service identity configuration.
   *
   * <p>This configuration is used only when the default realm ({@code DEFAULT_REALM_KEY}) has no
   * explicit configuration. It serves as a fallback for development scenarios where credentials are
   * obtained from the environment without requiring explicit configuration.
   *
   * @return the default resolvable service identity configuration
   */
  static ResolvableServiceIdentityConfiguration defaultConfiguration() {
    return new ResolvableServiceIdentityConfiguration() {
      // Returns empty for all methods - no explicit configuration available
      // Subclasses like AwsIamServiceIdentityConfiguration handle environment credentials
    };
  }
}
