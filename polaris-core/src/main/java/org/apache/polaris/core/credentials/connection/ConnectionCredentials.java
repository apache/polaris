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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.immutables.PolarisImmutable;

/**
 * Encapsulates credentials and configuration needed to connect to external federated catalogs.
 *
 * <p>Similar to {@link StorageAccessConfig} for storage, this class holds the credentials and
 * properties required for Polaris to authenticate with remote catalog services (e.g., AWS Glue,
 * other Iceberg REST catalogs).
 *
 * <p>Credentials may be temporary and include an expiration time.
 *
 * <p><b>Note:</b> This interface currently includes only {@code credentials} and {@code expiresAt}.
 * Additional fields like {@code extraProperties} and {@code internalProperties} (similar to {@link
 * StorageAccessConfig}) are not included for now but can be added later if needed for more complex
 * credential scenarios.
 */
@PolarisImmutable
public interface ConnectionCredentials {
  /** Sensitive credential properties (e.g., access keys, tokens). */
  Map<String, String> credentials();

  /** Optional expiration time for the credentials. */
  Optional<Instant> expiresAt();

  /**
   * Get a credential value by property key.
   *
   * @param key the credential property to retrieve
   * @return the credential value, or null if not present
   */
  default String get(CatalogAccessProperty key) {
    return credentials().get(key.getPropertyName());
  }

  static ConnectionCredentials.Builder builder() {
    return ImmutableConnectionCredentials.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder putCredential(String key, String value);

    @CanIgnoreReturnValue
    Builder expiresAt(Instant expiresAt);

    default Builder put(CatalogAccessProperty key, String value) {
      if (key.isExpirationTimestamp()) {
        expiresAt(Instant.ofEpochMilli(Long.parseLong(value)));
      }

      if (key.isCredential()) {
        return putCredential(key.getPropertyName(), value);
      }
      return this;
    }

    ConnectionCredentials build();
  }
}
