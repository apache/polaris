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

/**
 * Explicit resolution components for {@link PolarisResolutionManifest#resolveSelections}.
 *
 * <p>Selections control which entity groups the resolver should fetch for a request.
 */
public enum Resolvable {
  /** Resolve the authenticated caller principal entity. */
  CALLER_PRINCIPAL,
  /** Resolve the caller's activated principal-role entities. */
  CALLER_PRINCIPAL_ROLES,
  /** Resolve catalog-role entities (e.g., roles attached in the reference catalog). */
  CATALOG_ROLES,
  /** Resolve the reference catalog entity. */
  REFERENCE_CATALOG,
  /** Resolve explicitly registered paths (via addPath/addPassthroughPath). */
  REQUESTED_PATHS,
  /**
   * Resolve any additional top-level entities explicitly registered via addTopLevelName, such as
   * catalog/principal/principal-role names used for authorization beyond the caller and reference
   * catalog.
   */
  REQUESTED_TOP_LEVEL_ENTITIES
}
