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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.storage.StorageAccessConfig;

/**
 * Encapsulates credentials and configuration needed to connect to external federated catalogs.
 *
 * <p>Similar to {@link StorageAccessConfig} for storage, this class holds the credentials and
 * properties required for Polaris to authenticate with remote catalog services (e.g., AWS Glue,
 * other Iceberg REST catalogs).
 *
 * <p>Credentials may be temporary and include an expiration time.
 */
public record ConnectionCredentials(Map<String, String> credentials, Optional<Instant> expiresAt) {

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private final Map<String, String> credentials = new HashMap<>();
    private Instant expiresAt;

    public Builder putCredential(String key, String value) {
      credentials.put(key, value);
      return this;
    }

    public Builder expiresAt(Instant expiresAt) {
      this.expiresAt = expiresAt;
      return this;
    }

    public Builder put(CatalogAccessProperty key, String value) {
      if (key.isExpirationTimestamp()) {
        expiresAt(Instant.ofEpochMilli(Long.parseLong(value)));
      }
      if (key.isCredential()) {
        return putCredential(key.getPropertyName(), value);
      }
      return this;
    }

    public ConnectionCredentials build() {
      return new ConnectionCredentials(Map.copyOf(credentials), Optional.ofNullable(expiresAt));
    }
  }
}
