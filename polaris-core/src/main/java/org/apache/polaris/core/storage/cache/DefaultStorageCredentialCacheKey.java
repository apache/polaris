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
package org.apache.polaris.core.storage.cache;

import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;

/**
 * Storage access config parameters for FILE and unknown storage backends. Contains the minimal set
 * of fields needed for cache key identity.
 */
@PolarisImmutable
public interface DefaultStorageCredentialCacheKey extends StorageCredentialCacheKey {

  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  @Nullable
  String storageConfigSerializedStr();

  @Value.Parameter(order = 3)
  boolean allowedListAction();

  @Value.Parameter(order = 4)
  Set<String> allowedReadLocations();

  @Value.Parameter(order = 5)
  Set<String> allowedWriteLocations();

  @Value.Parameter(order = 6)
  Optional<String> refreshCredentialsEndpoint();

  static DefaultStorageCredentialCacheKey of(
      String realmId,
      @Nullable String storageConfigSerializedStr,
      boolean allowedListAction,
      Set<String> allowedReadLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {
    return ImmutableDefaultStorageCredentialCacheKey.of(
        realmId,
        storageConfigSerializedStr,
        allowedListAction,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint);
  }
}
