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
package org.apache.polaris.service.catalog;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.EnumSet;
import java.util.Optional;
import org.apache.polaris.core.entity.CatalogEntity;

/**
 * Resolves the optimal {@link AccessDelegationMode} to use based on the client's requested modes
 * and the catalog's capabilities.
 */
public interface AccessDelegationModeResolver {

  /**
   * Resolves the optimal access delegation mode based on the requested modes and catalog
   * capabilities.
   *
   * @param requestedModes The set of delegation modes requested by the client
   * @param catalogEntity The catalog entity, used to determine storage configuration and
   *     capabilities
   * @return The resolved access delegation mode, or empty if no suitable mode is available.
   * @throws IllegalArgumentException if the client requests a mode that is explicitly disallowed
   *     for the catalog and no viable fallback mode exists (e.g., {@link
   *     AccessDelegationMode#VENDED_CREDENTIALS} on an external catalog whose credential vending
   *     feature is disabled).
   */
  @Nonnull
  Optional<AccessDelegationMode> resolve(
      @Nonnull EnumSet<AccessDelegationMode> requestedModes, @Nullable CatalogEntity catalogEntity);
}
