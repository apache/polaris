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
package org.apache.polaris.core.storage;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

@PolarisImmutable
public interface StorageAccessConfig {
  Map<String, String> credentials();

  Map<String, String> extraProperties();

  /**
   * Configuration properties that are relevant only to the Polaris Server, but not to clients.
   * These properties override corresponding entries from {@link #extraProperties()}.
   */
  Map<String, String> internalProperties();

  Optional<Instant> expiresAt();

  /**
   * Indicates whether the storage integration subsystem that produced this object is capable of
   * credential vending in principle.
   */
  @Value.Default
  default boolean supportsCredentialVending() {
    return true;
  }

  default String get(StorageAccessProperty key) {
    if (key.isCredential()) {
      return credentials().get(key.getPropertyName());
    } else {
      String value = internalProperties().get(key.getPropertyName());
      return value != null ? value : extraProperties().get(key.getPropertyName());
    }
  }

  static StorageAccessConfig.Builder builder() {
    return ImmutableStorageAccessConfig.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder putCredential(String key, String value);

    @CanIgnoreReturnValue
    Builder putExtraProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putInternalProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder expiresAt(Instant expiresAt);

    @CanIgnoreReturnValue
    Builder supportsCredentialVending(boolean supportsCredentialVending);

    default Builder put(StorageAccessProperty key, String value) {
      if (key.isExpirationTimestamp()) {
        expiresAt(Instant.ofEpochMilli(Long.parseLong(value)));
      }

      if (key.isCredential()) {
        return putCredential(key.getPropertyName(), value);
      } else {
        return putExtraProperty(key.getPropertyName(), value);
      }
    }

    StorageAccessConfig build();
  }
}
