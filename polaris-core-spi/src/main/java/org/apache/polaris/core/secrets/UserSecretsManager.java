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
package org.apache.polaris.core.secrets;

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.entity.PolarisEntityCore;

/**
 * Manages secrets specified by users of the Polaris API, either directly or as an intermediary
 * layer between Polaris and external secret-management systems. Such secrets are distinct from
 * "service-level" secrets that pertain to the Polaris service itself which would be more statically
 * configured system-wide. In contrast, user-owned secrets are handled dynamically as part of
 * runtime API requests.
 */
public interface UserSecretsManager {
  /**
   * Persist the {@code secret} under a new URN {@code secretUrn} and return a {@code
   * SecretReference} that can subsequently be used by this same UserSecretsManager to retrieve the
   * original secret. The {@code forEntity} is provided for an implementation to extract other
   * identifying metadata such as entity type, id, name, etc., to store alongside the remotely
   * stored secret to facilitate operational management of the secrets outside of the core Polaris
   * service (for example, to perform garbage-collection if the Polaris service fails to delete
   * managed secrets in the external system when associated entities are deleted).
   *
   * @param secret The secret to store
   * @param forEntity The PolarisEntity that is associated with the secret
   * @return A reference object that can be used to retrieve the secret which is safe to store in
   *     its entirety within a persisted PolarisEntity
   */
  @Nonnull
  SecretReference writeSecret(@Nonnull String secret, @Nonnull PolarisEntityCore forEntity);

  /**
   * Retrieve a secret using the {@code secretReference}. See {@link SecretReference} for details
   * about identifiers and payloads.
   *
   * @param secretReference Reference object for retrieving the original secret
   * @return The stored secret, or null if it no longer exists
   */
  @Nonnull
  String readSecret(@Nonnull SecretReference secretReference);

  /**
   * Delete a stored secret. See {@link SecretReference} for details about identifiers and payloads.
   *
   * @param secretReference Reference object for retrieving the original secret
   */
  void deleteSecret(@Nonnull SecretReference secretReference);

  /**
   * Builds a URN string from the given secret manager type and type-specific identifier.
   *
   * @param typeSpecificIdentifier The type-specific identifier (colon-separated alphanumeric
   *     components with underscores and hyphens).
   * @return The constructed URN string.
   */
  @Nonnull
  default String buildUrn(
      @Nonnull String secretManagerType, @Nonnull String typeSpecificIdentifier) {
    return SecretReference.buildUrnString(secretManagerType, typeSpecificIdentifier);
  }
}
