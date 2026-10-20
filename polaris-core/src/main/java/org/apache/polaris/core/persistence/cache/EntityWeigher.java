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
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jspecify.annotations.NonNull;

/**
 * A {@link Weigher} implementation that weighs {@link ResolvedPolarisEntity} objects by the
 * approximate size of the entity object and of its grant records.
 */
public final class EntityWeigher implements Weigher<Long, ResolvedPolarisEntity> {

  public static final EntityWeigher INSTANCE = new EntityWeigher();

  /* Represents the approximate size of an entity beyond the properties */
  private static final long APPROXIMATE_ENTITY_OVERHEAD = 1000;

  /* Represents the amount of bytes that a character is expected to take up */
  private static final long APPROXIMATE_BYTES_PER_CHAR = 3;

  /*
   * Represents the approximate size of a grant record: 12-byte object header, four longs and an int
   * (36 bytes), plus the reference to it held by the list.
   */
  private static final long APPROXIMATE_GRANT_RECORD_SIZE = 64;

  private EntityWeigher() {}

  /**
   * Computes the weight of a given entity. The unit here is not exactly bytes, but it's close.
   *
   * @param key The entity's key; not used
   * @param value The entity to be cached
   * @return The weight of the entity
   */
  @Override
  public @NonNegative int weigh(@NonNull Long key, ResolvedPolarisEntity value) {
    long weight =
        APPROXIMATE_ENTITY_OVERHEAD
            + ((long) value.getEntity().getName().length() * APPROXIMATE_BYTES_PER_CHAR)
            + ((long) value.getEntity().getProperties().length() * APPROXIMATE_BYTES_PER_CHAR)
            + ((long) value.getEntity().getInternalProperties().length()
                * APPROXIMATE_BYTES_PER_CHAR)
            + ((long) value.getGrantRecordsAsGrantee().size() * APPROXIMATE_GRANT_RECORD_SIZE)
            + ((long) value.getGrantRecordsAsSecurable().size() * APPROXIMATE_GRANT_RECORD_SIZE);
    // Caffeine weights are ints; a huge grant list must not overflow into a negative weight
    return (int) Math.min(weight, Integer.MAX_VALUE);
  }
}
