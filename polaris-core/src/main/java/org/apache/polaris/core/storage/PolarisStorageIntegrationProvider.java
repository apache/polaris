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
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.storage.cache.DefaultStorageAccessConfigParameters;
import org.apache.polaris.core.storage.cache.StorageAccessConfigParameters;

/**
 * Factory interface that knows how to construct a {@link PolarisStorageIntegration} given a {@link
 * PolarisStorageConfigurationInfo}.
 */
public interface PolarisStorageIntegrationProvider {
  <T extends PolarisStorageConfigurationInfo>
      @Nullable PolarisStorageIntegration<T> getStorageIntegrationForConfig(
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo);

  /**
   * Builds storage access config parameters for credential caching. Different storage backends may
   * include different fields based on which parameters actually affect the vended credentials.
   *
   * <p>The default implementation uses {@link DefaultStorageAccessConfigParameters} which excludes
   * principal and context. Implementations should override to dispatch to backend-specific
   * parameter building logic.
   */
  default StorageAccessConfigParameters buildStorageAccessConfigParameters(
      @Nonnull String realmId,
      @Nonnull PolarisEntity entity,
      @Nonnull RealmConfig realmConfig,
      boolean allowListOperation,
      @Nonnull Set<String> allowedReadLocations,
      @Nonnull Set<String> allowedWriteLocations,
      @Nonnull Optional<String> refreshCredentialsEndpoint,
      @Nonnull PolarisPrincipal polarisPrincipal,
      @Nonnull CredentialVendingContext credentialVendingContext) {
    return DefaultStorageAccessConfigParameters.of(
        realmId,
        entity,
        allowListOperation,
        allowedReadLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint);
  }
}
