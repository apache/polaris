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

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.dao.entity.ScopedCredentialsResult;

public class StorageCredentialsVendor {

  private final PolarisCredentialVendor polarisCredentialVendor;
  private final CallContext callContext;

  public StorageCredentialsVendor(
      PolarisCredentialVendor polarisCredentialVendor, CallContext callContext) {
    this.polarisCredentialVendor = polarisCredentialVendor;
    this.callContext = callContext;
  }

  public RealmContext getRealmContext() {
    return callContext.getRealmContext();
  }

  public RealmConfig getRealmConfig() {
    return callContext.getRealmConfig();
  }

  /**
   * Get sub-scoped credentials for an entity against the provided allowed read and write locations.
   *
   * @param entity the entity
   * @param allowListOperation whether to allow LIST operation on the allowedReadLocations and
   *     allowedWriteLocations
   * @param allowedReadLocations a set of allowed to read locations
   * @param allowedWriteLocations a set of allowed to write locations
   * @param refreshCredentialsEndpoint an optional endpoint to use for refreshing credentials. If
   *     supported by the storage type it will be returned to the client in the appropriate
   *     properties. The endpoint may be relative to the base URI and the client is responsible for
   *     handling the relative path
   * @return an enum map containing the scoped credentials
   */
  @Nonnull
  public ScopedCredentialsResult getSubscopedCredsForEntity(
      @Nonnull PolarisEntity entity,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint) {
    return polarisCredentialVendor.getSubscopedCredsForEntity(
        callContext.getPolarisCallContext(),
        entity.getCatalogId(),
        entity.getId(),
        entity.getType(),
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint);
  }
}
