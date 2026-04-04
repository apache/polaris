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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.entity.PolarisEntity;

/**
 * SPI for vending scoped storage credentials.
 *
 * <p>The default implementation resolves storage configuration from the entity hierarchy, obtains a
 * {@link PolarisStorageIntegration} via {@link PolarisStorageIntegrationProvider}, and vends
 * credentials in-memory without persistence involvement.
 *
 * <p>Custom implementations may integrate with the persistence layer (e.g. via {@link
 * org.apache.polaris.core.persistence.IntegrationPersistence}) to support stateful credential
 * pooling or other deployment-specific credential management strategies.
 */
public interface PolarisCredentialVendor {

  /**
   * Vend scoped storage credentials for the given entity path and locations.
   *
   * @param resolvedEntityPath the fully resolved entity hierarchy, ordered from catalog to leaf
   *     entity. Implementations may walk this path to find storage configuration, resolve
   *     overrides, or locate entity-linked credential state.
   * @param locations the set of storage location URIs to scope credentials to
   * @param storageActions the storage operations (READ, WRITE, LIST, DELETE, ALL) to scope
   *     credentials for
   * @param refreshCredentialsEndpoint optional endpoint URL for clients to refresh credentials
   * @return a {@link StorageAccessConfig} containing the scoped credentials and metadata
   */
  @Nonnull
  StorageAccessConfig getStorageAccessConfig(
      @Nonnull List<PolarisEntity> resolvedEntityPath,
      @Nonnull Set<String> locations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull Optional<String> refreshCredentialsEndpoint);
}
