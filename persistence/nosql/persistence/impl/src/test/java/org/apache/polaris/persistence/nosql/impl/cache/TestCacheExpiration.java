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
package org.apache.polaris.persistence.nosql.impl.cache;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.cacheKeyValueObjRead;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.ImmutableSimpleTestObj;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheExpiration {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected String realmId;

  @BeforeEach
  protected void setUp() {
    realmId = "42";
  }

  @Test
  public void cachingObjectsExpiration() {
    var currentTime = new AtomicLong(1234L);

    var backend =
        new CaffeineCacheBackend(
            CacheConfig.BuildableCacheConfig.builder()
                .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(8)).build())
                .clockNanos(() -> MICROSECONDS.toNanos(currentTime.get()))
                .build(),
            Optional.empty());

    var id = 100L;

    var defaultCachingObj = ImmutableDefaultCachingObj.builder().id(id++).value("def").build();
    var nonCachingObj = ImmutableNonCachingObj.builder().id(id++).value("foo").build();
    var dynamicCachingObj =
        ImmutableDynamicCachingObj.builder().id(id++).thatExpireTimestamp(2L).build();
    var stdObj = ImmutableSimpleTestObj.builder().id(id).text("foo").build();

    backend.put(realmId, defaultCachingObj);
    backend.put(realmId, nonCachingObj);
    backend.put(realmId, dynamicCachingObj);
    backend.put(realmId, stdObj);

    var cacheMap = backend.cache.asMap();

    soft.assertThat(cacheMap)
        .doesNotContainKey(cacheKeyValueObjRead(realmId, objRef(nonCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(dynamicCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(defaultCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(stdObj)))
        .hasSize(3);

    soft.assertThat(backend.get(realmId, objRef(nonCachingObj))).isNull();
    soft.assertThat(backend.get(realmId, objRef(dynamicCachingObj))).isEqualTo(dynamicCachingObj);
    soft.assertThat(backend.get(realmId, objRef(defaultCachingObj))).isEqualTo(defaultCachingObj);
    soft.assertThat(backend.get(realmId, objRef(stdObj))).isEqualTo(stdObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(cacheKeyValueObjRead(realmId, objRef(nonCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(dynamicCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(defaultCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(stdObj)))
        .hasSize(3);

    // increment clock by one - "dynamic" object should still be present

    currentTime.addAndGet(1);

    soft.assertThat(backend.get(realmId, objRef(nonCachingObj))).isNull();
    soft.assertThat(backend.get(realmId, objRef(dynamicCachingObj))).isEqualTo(dynamicCachingObj);
    soft.assertThat(backend.get(realmId, objRef(defaultCachingObj))).isEqualTo(defaultCachingObj);
    soft.assertThat(backend.get(realmId, objRef(stdObj))).isEqualTo(stdObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(cacheKeyValueObjRead(realmId, objRef(nonCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(dynamicCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(defaultCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(stdObj)))
        .hasSize(3);

    // increment clock by one again - "dynamic" object should go away

    currentTime.addAndGet(1);

    soft.assertThat(backend.get(realmId, objRef(nonCachingObj))).isNull();
    soft.assertThat(backend.get(realmId, objRef(dynamicCachingObj))).isNull();
    soft.assertThat(backend.get(realmId, objRef(defaultCachingObj))).isEqualTo(defaultCachingObj);
    soft.assertThat(backend.get(realmId, objRef(stdObj))).isEqualTo(stdObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(cacheKeyValueObjRead(realmId, objRef(nonCachingObj)))
        .doesNotContainKey(cacheKeyValueObjRead(realmId, objRef(dynamicCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(defaultCachingObj)))
        .containsKey(cacheKeyValueObjRead(realmId, objRef(stdObj)));
    // note: Caffeine's cache-map incorrectly reports a size of 3 here, although the map itself only
    // returns the only left object
  }
}
