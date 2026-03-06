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

import java.util.List;
import org.apache.polaris.core.entity.PolarisEntityType;

/**
 * Simple class to represent a path within a catalog
 *
 * @param key canonical path key (entity names + terminal entity type)
 * @param optional true if this path is optional, i.e. failing to fully resolve it is not an error
 */
public record ResolverPath(ResolvedPathKey key, boolean optional) {

  public ResolverPath {
    key = new ResolvedPathKey(key.entityNames(), key.entityType());
  }

  /**
   * Constructor for a non-optional path.
   *
   * @param entityNames set of entity names, all are namespaces except the last one
   * @param lastEntityType type of the last entity
   */
  public ResolverPath(List<String> entityNames, PolarisEntityType lastEntityType) {
    this(new ResolvedPathKey(entityNames, lastEntityType), false);
  }

  public ResolverPath(
      List<String> entityNames, PolarisEntityType lastEntityType, boolean optional) {
    this(new ResolvedPathKey(entityNames, lastEntityType), optional);
  }

  /** Compatibility accessor for existing call sites. */
  public List<String> entityNames() {
    return key.entityNames();
  }

  /** Compatibility accessor for existing call sites. */
  public PolarisEntityType lastEntityType() {
    return key.entityType();
  }
}
