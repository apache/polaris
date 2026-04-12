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
import java.util.List;
import org.apache.polaris.core.entity.PolarisEntity;

/**
 * SPI that returns a {@link PolarisStorageIntegration} for a resolved entity path.
 *
 * <p>This is the single entry point used on the credential-vending path. The default implementation
 * walks the resolved hierarchy to find the entity carrying storage configuration and returns the
 * corresponding integration. Custom implementations may instead consult deployment-specific state
 * (e.g. an entity-linked credential lease) to return a different integration per entity.
 */
public interface PolarisStorageIntegrationProvider {

  /**
   * Return the {@link PolarisStorageIntegration} to use for vending credentials against the given
   * resolved entity path. Implementations are free to walk the path to locate storage
   * configuration, resolve overrides, or consult persistence-side state.
   */
  @Nullable
  PolarisStorageIntegration<?> getStorageIntegration(
      @Nonnull List<PolarisEntity> resolvedEntityPath);
}
