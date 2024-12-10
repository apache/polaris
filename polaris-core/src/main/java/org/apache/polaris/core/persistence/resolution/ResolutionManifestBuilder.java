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
package org.apache.polaris.core.persistence.resolution;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.EntityNotFoundException;
import org.apache.polaris.core.persistence.resolver.ResolverPath;

public interface ResolutionManifestBuilder {
  @CanIgnoreReturnValue
  ResolutionManifestBuilder withRootContainerEntity(
      ResolvedPolarisEntity simulatedResolvedRootContainerEntity);

  /** Adds a name of a top-level entity (Catalog, Principal, PrincipalRole) to be resolved. */
  @CanIgnoreReturnValue
  ResolutionManifestBuilder addTopLevelName(
      String topLevelEntityName, PolarisEntityType entityType, boolean optional);

  /**
   * Adds a path that will be statically resolved with the primary Resolver when resolveAll() is
   * called, and which contributes to the resolution status of whether all paths have successfully
   * resolved.
   *
   * @param catalogRoleName the friendly lookup key for retrieving resolvedPaths after resolveAll()
   */
  @CanIgnoreReturnValue
  ResolutionManifestBuilder addPath(ResolverPath resolverPath, String catalogRoleName);

  /**
   * Adds a path that will be statically resolved with the primary Resolver when resolveAll() is
   * called, and which contributes to the resolution status of whether all paths have successfully
   * resolved.
   *
   * @param namespace the friendly lookup key for retrieving resolvedPaths after resolveAll()
   */
  @CanIgnoreReturnValue
  ResolutionManifestBuilder addPath(ResolverPath resolverPath, Namespace namespace);

  /**
   * Adds a path that will be statically resolved with the primary Resolver when resolveAll() is
   * called, and which contributes to the resolution status of whether all paths have successfully
   * resolved.
   *
   * @param tableIdentifier the friendly lookup key for retrieving resolvedPaths after resolveAll()
   */
  @CanIgnoreReturnValue
  ResolutionManifestBuilder addPath(ResolverPath resolverPath, TableIdentifier tableIdentifier);

  /**
   * Adds a path that is allowed to be dynamically resolved with a new Resolver when
   * getPassthroughResolvedPath is called. These paths are also included in the primary static
   * resolution set resolved during resolveAll().
   */
  @CanIgnoreReturnValue
  ResolutionManifestBuilder addPassthroughPath(
      ResolverPath resolverPath, TableIdentifier tableIdentifier);

  /**
   * Adds a path that is allowed to be dynamically resolved with a new Resolver when
   * getPassthroughResolvedPath is called. These paths are also included in the primary static
   * resolution set resolved during resolveAll().
   */
  @CanIgnoreReturnValue
  ResolutionManifestBuilder addPassthroughPath(ResolverPath resolverPath, Namespace namespace);

  @CanIgnoreReturnValue
  ResolutionManifestBuilder notFoundExceptionMapper(
      Function<EntityNotFoundException, RuntimeException> notFoundExceptionMapper);

  ResolutionManifest buildResolved(PolarisEntitySubType subType);

  ResolutionManifest buildResolved();
}
