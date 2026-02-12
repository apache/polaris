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
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;

/**
 * Resolves the optimal {@link AccessDelegationMode} to use based on the client's requested modes
 * and the catalog's capabilities.
 *
 * <p>The selection algorithm is:
 *
 * <ol>
 *   <li>If no delegation mode is requested, use {@link AccessDelegationMode#UNKNOWN}
 *   <li>If exactly one delegation mode is requested, use that mode (if supported)
 *   <li>If the requested modes include both {@link AccessDelegationMode#VENDED_CREDENTIALS} and
 *       {@link AccessDelegationMode#REMOTE_SIGNING}:
 *       <ol>
 *         <li>Check if STS is available for the catalog's storage configuration
 *         <li>If STS is available and credential subscoping is not skipped, use {@link
 *             AccessDelegationMode#VENDED_CREDENTIALS}
 *         <li>Otherwise, use {@link AccessDelegationMode#REMOTE_SIGNING}
 *       </ol>
 * </ol>
 *
 * <p>This resolver improves upon the simple mode selection by considering:
 *
 * <ul>
 *   <li>STS availability from the catalog's {@link AwsStorageConfigurationInfo}
 *   <li>Feature configuration settings for credential subscoping
 * </ul>
 */
public interface AccessDelegationModeResolver {

  /**
   * Resolves the optimal access delegation mode based on the requested modes and catalog
   * capabilities.
   *
   * @param requestedModes The set of delegation modes requested by the client
   * @param catalogEntity The catalog entity, used to determine storage configuration and
   *     capabilities
   * @return The resolved access delegation mode
   */
  @Nonnull
  AccessDelegationMode resolve(
      @Nonnull EnumSet<AccessDelegationMode> requestedModes,
      @Nullable CatalogEntity catalogEntity);

  /**
   * Convenience method to resolve and return an EnumSet containing just the resolved mode.
   *
   * @param requestedModes The set of delegation modes requested by the client
   * @param catalogEntity The catalog entity
   * @return An EnumSet containing the single resolved mode
   */
  @Nonnull
  default EnumSet<AccessDelegationMode> resolveToSet(
      @Nonnull EnumSet<AccessDelegationMode> requestedModes,
      @Nullable CatalogEntity catalogEntity) {
    AccessDelegationMode resolved = resolve(requestedModes, catalogEntity);
    return EnumSet.of(resolved);
  }
}
