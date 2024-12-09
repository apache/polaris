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
package org.apache.polaris.core.persistence.resolver;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nonnull;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * REST request resolver, allows to resolve all entities referenced directly or indirectly by in
 * incoming rest request, Once resolved, the request can be authorized.
 */
public interface ResolverBuilder {
  /**
   * Add a top-level entity to resolve. If the entity type is a catalog role, we also expect that a
   * reference catalog entity was specified at creation time, else we will assert. That catalog role
   * entity will be resolved from there. We will fail the entire resolution process if that entity
   * cannot be resolved. If this is not expected, use addOptionalEntityByName() instead.
   *
   * @param entityType the type of the entity, either a principal, a principal role, a catalog or a
   *     catalog role.
   * @param entityName the name of the entity
   */
  @CanIgnoreReturnValue
  ResolverBuilder addEntityByName(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName);

  /**
   * Add an optional top-level entity to resolve. If the entity type is a catalog role, we also
   * expect that a reference catalog entity was specified at creation time, else we will assert.
   * That catalog role entity will be resolved from there. If the entity cannot be resolved, we will
   * not fail the resolution process
   *
   * @param entityType the type of the entity, either a principal, a principal role, a catalog or a
   *     catalog role.
   * @param entityName the name of the entity
   */
  @CanIgnoreReturnValue
  ResolverBuilder addOptionalEntityByName(
      @Nonnull PolarisEntityType entityType, @Nonnull String entityName);

  /**
   * Add a path to resolve
   *
   * @param path path to resolve
   */
  @CanIgnoreReturnValue
  ResolverBuilder addPath(@Nonnull ResolverPath path);

  /**
   * Run the resolution process and return the status, either an error or success
   *
   * <p>Resolution might be working using multiple passes when using the cache since anything we
   * find in the cache might have changed in the backend store.
   *
   * <p>For each pass we will
   *
   * <ul>
   *   <li>go over all entities and call EntityCache.getOrLoad...() on these entities, including all
   *       paths.
   *   <li>split these entities into 3 groups:
   *       <ul>
   *         <li>dropped or purged. We will return an error for these.
   *         <li>to be validated entities, they were found in the cache. For those we need to ensure
   *             that the entity id, its name and parent id has not changed. If yes we need to
   *             perform another pass.
   *         <li>reloaded from backend, so the entity is validated. Validated entities will not be
   *             validated again
   *       </ul>
   * </ul>
   *
   * @return the resolver instance. If successfull, all entities have been resolved and the
   *     getResolvedXYZ() method can be called.
   */
  Resolver buildResolved();
}
