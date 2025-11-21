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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.persistence.nosql.api.obj.VersionedTestObj;
import org.apache.polaris.persistence.nosql.api.ref.ImmutableReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestDistributedInvalidations {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AtomicLong clockNanos;

  CaffeineCacheBackend backend1noSpy;
  CaffeineCacheBackend backend2noSpy;
  CaffeineCacheBackend backend1;
  CaffeineCacheBackend backend2;

  protected CacheBackend distributed1;
  protected CacheBackend distributed2;

  protected DistributedCacheInvalidation.Sender sender1;
  protected DistributedCacheInvalidation.Sender sender2;

  protected String realmId;

  @BeforeEach
  public void setup() {
    realmId = "42";

    clockNanos = new AtomicLong();

    backend1noSpy =
        (CaffeineCacheBackend)
            PersistenceCaches.newBackend(
                CacheConfig.BuildableCacheConfig.builder()
                    .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(16)).build())
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofSeconds(1))
                    .clockNanos(clockNanos::get)
                    .build(),
                Optional.empty());
    backend2noSpy =
        (CaffeineCacheBackend)
            PersistenceCaches.newBackend(
                CacheConfig.BuildableCacheConfig.builder()
                    .sizing(CacheSizing.builder().fixedSize(MemorySize.ofMega(16)).build())
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofSeconds(1))
                    .clockNanos(clockNanos::get)
                    .build(),
                Optional.empty());

    backend1 = spy(backend1noSpy);
    backend2 = spy(backend2noSpy);

    // reversed!
    sender1 = spy(delegate(backend2));
    sender2 = spy(delegate(backend1));

    distributed1 = new DistributedInvalidationsCacheBackend(backend1, sender1);
    distributed2 = new DistributedInvalidationsCacheBackend(backend2, sender2);
  }

  @Test
  public void obj() {
    var obj1 = VersionedTestObj.builder().id(100).versionToken("1").someValue("hello").build();
    var obj2 = VersionedTestObj.builder().id(100).versionToken("2").someValue("again").build();

    distributed1.put(realmId, obj1);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putLocal(realmId, obj1);
    verify(backend2).remove(realmId, objRef(obj1));
    verify(sender1).evictObj(realmId, objRef(obj1));
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    soft.assertThat(backend1noSpy.get(realmId, objRef(obj1))).isEqualTo(obj1);
    soft.assertThat(backend2noSpy.get(realmId, objRef(obj1))).isNull();

    // Simulate that backend2 loaded obj1 in the meantime
    backend2noSpy.put(realmId, obj1);
    soft.assertThat(backend2noSpy.get(realmId, objRef(obj1))).isEqualTo(obj1);

    distributed1.put(realmId, obj2);
    soft.assertThat(backend2noSpy.get(realmId, objRef(obj1))).isNull();

    verify(backend1).cachePut(any(), any());
    verify(backend1).putLocal(realmId, obj2);
    verify(backend2).remove(realmId, objRef(obj1));
    verify(sender1).evictObj(realmId, objRef(obj2));
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Simulate that backend2 loaded obj2 in the meantime
    backend2noSpy.put(realmId, obj2);
    soft.assertThat(backend2noSpy.get(realmId, objRef(obj2))).isEqualTo(obj2);

    // update to same object (still a removal for backend2)

    distributed1.put(realmId, obj2);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putLocal(realmId, obj2);
    verify(backend2).remove(realmId, objRef(obj2));
    verify(sender1).evictObj(realmId, objRef(obj2));
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Verify that ref2 has not been removed (same hash)
    soft.assertThat(backend2noSpy.get(realmId, objRef(obj2))).isNull();

    // remove object

    distributed1.remove(realmId, objRef(obj2));

    verify(backend1).remove(realmId, objRef(obj2));
    verify(backend2).remove(realmId, objRef(obj2));
    verify(sender1).evictObj(realmId, objRef(obj2));
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();
  }

  @Test
  public void reference() {
    var ref1 =
        ImmutableReference.builder()
            .name("refs/foo/bar")
            .pointer(objRef(SimpleTestObj.TYPE, 100, 1))
            .createdAtMicros(0)
            .previousPointers()
            .build();
    var ref2 =
        ImmutableReference.builder()
            .from(ref1)
            .pointer(objRef(SimpleTestObj.TYPE, 101, 1))
            .previousPointers()
            .build();

    distributed1.putReference(realmId, ref1);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putReferenceLocal(realmId, ref1);
    verify(backend2).removeReference(realmId, ref1.name());
    verify(sender1).evictReference(realmId, ref1.name());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    soft.assertThat(backend1noSpy.getReference(realmId, ref1.name())).isEqualTo(ref1);
    soft.assertThat(backend2noSpy.getReference(realmId, ref1.name())).isNull();

    // Simulate that backend2 loaded ref1 in the meantime
    backend2noSpy.putReference(realmId, ref1);
    soft.assertThat(backend2noSpy.getReference(realmId, ref1.name())).isEqualTo(ref1);

    distributed1.putReference(realmId, ref2);
    soft.assertThat(backend2noSpy.getReference(realmId, ref1.name())).isNull();

    verify(backend1).cachePut(any(), any());
    verify(backend1).putReferenceLocal(realmId, ref2);
    verify(backend2).removeReference(realmId, ref1.name());
    verify(backend2).removeReference(realmId, ref1.name());
    verify(sender1).evictReference(realmId, ref2.name());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Simulate that backend2 loaded ref2 in the meantime
    backend2noSpy.putReference(realmId, ref2);
    soft.assertThat(backend2noSpy.getReference(realmId, ref2.name())).isEqualTo(ref2);

    // update to same reference (no change for backend2)

    distributed1.putReference(realmId, ref2);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putReferenceLocal(realmId, ref2);
    verify(backend2).removeReference(realmId, ref2.name());
    verify(sender1).evictReference(realmId, ref2.name());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Verify that ref2 has been removed in backend2
    soft.assertThat(backend2noSpy.getReference(realmId, ref2.name())).isNull();

    // remove reference

    distributed1.removeReference(realmId, ref2.name());

    verify(backend1).removeReference(realmId, ref2.name());
    verify(backend2).removeReference(realmId, ref2.name());
    verify(sender1).evictReference(realmId, ref2.name());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();
  }

  private void resetAll() {
    reset(backend1);
    reset(backend2);
    reset(sender1);
    reset(sender2);
  }

  protected static DistributedCacheInvalidation.Sender delegate(CacheBackend backend) {
    return new DistributedCacheInvalidation.Sender() {
      @Override
      public void evictObj(@Nonnull String realmId, @Nonnull ObjRef objRef) {
        backend.remove(realmId, objRef);
      }

      @Override
      public void evictReference(@Nonnull String realmId, @Nonnull String refName) {
        backend.removeReference(realmId, refName);
      }
    };
  }
}
