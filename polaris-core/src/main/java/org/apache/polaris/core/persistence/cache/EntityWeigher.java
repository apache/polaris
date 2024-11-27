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
package org.apache.polaris.core.persistence.cache;

import com.github.benmanes.caffeine.cache.Weigher;
import org.checkerframework.checker.index.qual.NonNegative;

/**
 * A {@link Weigher} implementation that weighs {@link EntityCacheEntry} objects by the approximate
 * size of the entity object.
 */
public class EntityWeigher implements Weigher<Long, EntityCacheEntry> {

  /** The amount of weight that is expected to roughly equate to 1MB of memory usage */
  public static final long WEIGHT_PER_MB = 1024 * 1024;

  /* Represents the approximate size of an entity beyond the properties */
  private static final int APPROXIMATE_ENTITY_OVERHEAD = 1000;

  /** Singleton instance */
  private static final EntityWeigher instance = new EntityWeigher();

  private EntityWeigher() {}

  /** Gets the singleton {@link EntityWeigher} */
  public static EntityWeigher getInstance() {
    return instance;
  }

  /**
   * Computes the weight of a given entity
   *
   * @param key The entity's key; not used
   * @param value The entity to be cached
   * @return The weight of the entity
   */
  @Override
  public @NonNegative int weigh(Long key, EntityCacheEntry value) {
    return APPROXIMATE_ENTITY_OVERHEAD
        + value.getEntity().getProperties().length()
        + value.getEntity().getInternalProperties().length();
  }

  /** Factory method to provide a typed Weigher */
  public static Weigher<Long, EntityCacheEntry> asWeigher() {
    return (Weigher<Long, EntityCacheEntry>) getInstance();
  }
}
