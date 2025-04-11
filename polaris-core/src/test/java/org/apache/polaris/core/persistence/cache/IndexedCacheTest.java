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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IndexedCacheTest {
  @Test
  void testInvalidateClearsIndexes() {
    IndexedCache<CacheKey, ResolvedPolarisEntity> cache =
        new IndexedCache.Builder<CacheKey, ResolvedPolarisEntity>()
            .primaryKey(e -> new IdKey(e.getEntity().getId()))
            .addSecondaryKey(e -> new NameKey(new EntityCacheByNameKey(e.getEntity())))
            .expireAfterAccess(Duration.ofHours(1))
            .maximumSize(100_000)
            .build();

    PolarisEntity polarisEntity = Mockito.mock(PolarisEntity.class);
    when(polarisEntity.getId()).thenReturn(4L);
    when(polarisEntity.getCatalogId()).thenReturn(5L);
    when(polarisEntity.getParentId()).thenReturn(6L);
    when(polarisEntity.getTypeCode()).thenReturn(99);
    when(polarisEntity.getName()).thenReturn("the name");

    ResolvedPolarisEntity resolvedPolarisEntity = Mockito.mock(ResolvedPolarisEntity.class);
    when(resolvedPolarisEntity.getEntity()).thenReturn(polarisEntity);

    // Load an entity into the cache, then invalidate it
    IdKey idKey = new IdKey(4L);
    NameKey nameKey = new NameKey(new EntityCacheByNameKey(polarisEntity));
    cache.get(nameKey, () -> resolvedPolarisEntity);
    cache.invalidate(idKey);

    // Verify that the entity is not in the cache anymore and that indexes are cleared
    assertThat(cache.getIfPresent(idKey)).isNull();
    assertThat(cache.getIfPresent(nameKey)).isNull();
    assertThat(cache.getIndexes()).isEmpty();
  }
}
