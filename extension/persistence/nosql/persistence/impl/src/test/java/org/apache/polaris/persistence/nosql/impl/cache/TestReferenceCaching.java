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

import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.polaris.ids.api.IdGenerator;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceAlreadyExistsException;
import org.apache.polaris.persistence.nosql.api.exceptions.ReferenceNotFoundException;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.persistence.nosql.api.ref.Reference;
import org.apache.polaris.persistence.nosql.testextension.BackendSpec;
import org.apache.polaris.persistence.nosql.testextension.PersistenceTestExtension;
import org.apache.polaris.persistence.nosql.testextension.PolarisPersistence;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({PersistenceTestExtension.class, SoftAssertionsExtension.class})
@BackendSpec
public class TestReferenceCaching {
  @InjectSoftAssertions protected SoftAssertions soft;

  Persistence wrapWithCache(Persistence persist, LongSupplier clockNanos) {
    return PersistenceCaches.newBackend(
            CacheConfig.BuildableCacheConfig.builder()
                .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(16)).build())
                .clockNanos(clockNanos)
                .referenceTtl(Duration.ofMinutes(1))
                .referenceNegativeTtl(Duration.ofSeconds(1))
                .build(),
            Optional.empty())
        .wrap(persist);
  }

  // Two caching `Persist` instances, using _independent_ cache backends.
  Persistence withCache1;
  Persistence withCache2;

  AtomicLong nowNanos;

  @PolarisPersistence IdGenerator idGenerator;

  @BeforeEach
  void wrapCaches(
      @PolarisPersistence(realmId = "2") Persistence persist1,
      @PolarisPersistence(realmId = "2") Persistence persist2) {
    nowNanos = new AtomicLong();
    withCache1 = wrapWithCache(persist1, nowNanos::get);
    withCache2 = wrapWithCache(persist2, nowNanos::get);
  }

  ObjRef newId() {
    return objRef(SimpleTestObj.TYPE, idGenerator.generateId(), 1);
  }

  /** Explicit cache-expiry via {@link Persistence#fetchReferenceForUpdate(String)}. */
  @Test
  public void referenceCacheInconsistency(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();

    // Create ref via instance 1
    var ref = withCache1.createReference(refName, Optional.of(newId()));

    // Populate cache in instance 2
    soft.assertThat(fetchRef(withCache2, ref.name())).isEqualTo(ref);

    // Update ref via instance 1
    var refUpdated =
        withCache1.updateReferencePointer(ref, objRef(SimpleTestObj.TYPE, 101, 1)).orElseThrow();
    soft.assertThat(refUpdated).isNotEqualTo(ref);

    soft.assertThat(fetchRef(withCache1, ref.name())).isEqualTo(refUpdated);
    // Other test instance did NOT update its cache
    soft.assertThat(fetchRef(withCache2, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refUpdated.pointer())
        .isEqualTo(ref.pointer())
        .isNotEqualTo(refUpdated.pointer());

    soft.assertThat(withCache2.fetchReferenceForUpdate(ref.name())).isEqualTo(refUpdated);
  }

  /** Reference cache TTL expiry. */
  @Test
  public void referenceCacheExpiry(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();

    // Create ref via instance 1
    var ref = withCache1.createReference(refName, Optional.of(newId()));

    // Populate cache in instance 2
    soft.assertThat(fetchRef(withCache2, ref.name())).isEqualTo(ref);

    // Update ref via instance 1
    var refUpdated = withCache1.updateReferencePointer(ref, newId()).orElseThrow();
    soft.assertThat(refUpdated).isNotEqualTo(ref);

    soft.assertThat(fetchRef(withCache1, ref.name())).isEqualTo(refUpdated);
    // Other test instance did NOT update its cache
    soft.assertThat(fetchRef(withCache2, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refUpdated.pointer())
        .isEqualTo(ref.pointer())
        .isNotEqualTo(refUpdated.pointer());

    //

    nowNanos.addAndGet(Duration.ofMinutes(2).toNanos());
    soft.assertThat(fetchRef(withCache2, ref.name())).isEqualTo(refUpdated);
  }

  /** Tests negative-cache behavior (non-existence of a reference). */
  @Test
  public void referenceCacheNegativeExpiry(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();

    // Populate both caches w/ negative entries
    soft.assertThatThrownBy(() -> fetchRef(withCache1, refName))
        .isInstanceOf(ReferenceNotFoundException.class);
    soft.assertThatThrownBy(() -> fetchRef(withCache2, refName))
        .isInstanceOf(ReferenceNotFoundException.class);

    // Create ref via instance 1
    var ref = withCache1.createReference(refName, Optional.of(newId()));

    // Cache 1 has "correct" entry
    soft.assertThat(fetchRef(withCache1, ref.name())).isEqualTo(ref);
    // Cache 2 has stale negative entry
    soft.assertThatThrownBy(() -> fetchRef(withCache2, refName))
        .isInstanceOf(ReferenceNotFoundException.class);

    // Expire negative cache entries
    nowNanos.addAndGet(Duration.ofSeconds(2).toNanos());
    soft.assertThat(fetchRef(withCache2, ref.name())).isEqualTo(ref);
  }

  @Test
  public void addReference(TestInfo testInfo) {
    var refName = testInfo.getTestMethod().orElseThrow().getName();

    // Create ref via instance 1
    var ref = withCache1.createReference(refName, Optional.of(newId()));

    // Try addReference via instance 2
    soft.assertThatThrownBy(() -> withCache2.createReference(refName, Optional.of(newId())))
        .isInstanceOf(ReferenceAlreadyExistsException.class);

    // Update ref via instance 1
    var refUpdated = withCache1.updateReferencePointer(ref, newId()).orElseThrow();
    soft.assertThat(refUpdated).isNotEqualTo(ref);

    soft.assertThat(fetchRef(withCache1, ref.name())).isEqualTo(refUpdated);
    // Other test instance DID populate its cache
    soft.assertThat(fetchRef(withCache2, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refUpdated.pointer())
        .isEqualTo(refUpdated.pointer());
  }

  static Reference fetchRef(Persistence persist, String refName) {
    return persist.fetchReference(refName);
  }
}
