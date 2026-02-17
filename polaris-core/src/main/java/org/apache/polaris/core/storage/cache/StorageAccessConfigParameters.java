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

import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.storage.CredentialVendingContext;

/**
 * Common interface for all storage access configuration parameter types used as cache keys. Each
 * storage backend defines its own implementation with only the fields that affect credential
 * vending for that backend.
 *
 * <p>Caffeine cache uses {@code equals()}/{@code hashCode()} for key lookup. Since Immutables
 * generates both based on the concrete class and its fields, different backend parameter types will
 * never collide even when sharing the same cache.
 *
 * <p>This interface also provides common accessor methods used by {@link
 * org.apache.polaris.core.storage.PolarisStorageIntegration#getSubscopedCreds} so that credential
 * vending implementations can read from the params object without casting.
 */
public interface StorageAccessConfigParameters {

  /** Whether LIST operations are allowed on the provided locations. */
  boolean allowedListAction();

  /** The set of storage location URIs that may be read. */
  Set<String> allowedReadLocations();

  /** The set of storage location URIs that may be written. */
  Set<String> allowedWriteLocations();

  /** An optional endpoint URL for clients to refresh credentials. */
  Optional<String> refreshCredentialsEndpoint();

  /**
   * Returns the principal name to embed in vended credentials (e.g. AWS role session name). For
   * backends that do not support this (GCP, Azure, FILE), this returns {@link Optional#empty()}.
   */
  default Optional<String> principalName() {
    return Optional.empty();
  }

  /**
   * Returns the credential vending context associated with these parameters. For backends that do
   * not support session tags (GCP, Azure, FILE), this returns {@link
   * CredentialVendingContext#empty()}.
   */
  default CredentialVendingContext credentialVendingContext() {
    return CredentialVendingContext.empty();
  }
}
