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

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.METER_CACHE_ADMIT_CAPACITY;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.METER_CACHE_CAPACITY;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.METER_CACHE_REJECTED_WEIGHT;
import static org.apache.polaris.persistence.nosql.impl.cache.CaffeineCacheBackend.METER_CACHE_WEIGHT;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.misc.types.memorysize.MemorySize;
import org.apache.polaris.persistence.nosql.api.cache.CacheConfig;
import org.apache.polaris.persistence.nosql.api.cache.CacheSizing;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junitpioneer.jupiter.RetryingTest;

public class TestCacheOvershoot {

  /** This simulates the production setup. */
  @RetryingTest(minSuccess = 5, maxAttempts = 10)
  // It may happen that the admitted weight is actually exceeded. Allow some failed iterations.
  public void testCacheOvershootDirectEviction() throws Exception {
    testCacheOvershoot(Runnable::run, true);
  }

  /** This test <em>illustrates</em> delayed eviction, leading to more heap usage than admitted. */
  @RepeatedTest(10) // consider the first repetition as a warmup (C1/C2)
  @Disabled("not production like")
  public void testCacheOvershootDelayedEviction() throws Exception {
    // Production uses Runnable::run, but that lets this test sometimes run way too
    // long, so we introduce some delay to simulate the case that eviction cannot keep up.
    testCacheOvershoot(t -> delayedExecutor(2, TimeUnit.MILLISECONDS).execute(t), false);
  }

  private void testCacheOvershoot(Executor evictionExecutor, boolean direct) throws Exception {
    var meterRegistry = new SimpleMeterRegistry();

    var config =
        CacheConfig.BuildableCacheConfig.builder()
            .sizing(
                CacheSizing.builder()
                    .fixedSize(MemorySize.ofMega(4))
                    .cacheCapacityOvershoot(0.1d)
                    .build())
            .build();
    var cache = new CaffeineCacheBackend(config, Optional.of(meterRegistry), evictionExecutor);

    var metersByName =
        meterRegistry.getMeters().stream()
            .collect(Collectors.toMap(m -> m.getId().getName(), Function.identity(), (a, b) -> a));
    assertThat(metersByName)
        .containsKeys(METER_CACHE_WEIGHT, METER_CACHE_ADMIT_CAPACITY, METER_CACHE_REJECTED_WEIGHT);
    var meterWeightReported = (Gauge) metersByName.get(METER_CACHE_WEIGHT);
    var meterAdmittedCapacity = (Gauge) metersByName.get(METER_CACHE_ADMIT_CAPACITY);
    var meterCapacity = (Gauge) metersByName.get(METER_CACHE_CAPACITY);
    var meterRejectedWeight = (DistributionSummary) metersByName.get(METER_CACHE_REJECTED_WEIGHT);

    var maxWeight = cache.capacityBytes();
    var admitWeight = cache.admitWeight();

    var str = Strings.repeat("a", 4096);

    var idGen = new AtomicLong();

    var numThreads = 8;

    for (int i = 0; i < maxWeight / 5000; i++) {
      cache.put("repo", SimpleTestObj.builder().id(idGen.incrementAndGet()).text(str).build());
    }

    assertThat(cache.currentWeightReported()).isLessThanOrEqualTo(maxWeight);
    assertThat(cache.rejections()).isEqualTo(0L);
    assertThat(meterWeightReported.value()).isGreaterThan(0d);
    assertThat(meterAdmittedCapacity.value()).isEqualTo((double) admitWeight);
    assertThat(meterCapacity.value())
        .isEqualTo((double) config.sizing().orElseThrow().fixedSize().orElseThrow().asLong());

    var seenAdmittedWeightExceeded = false;
    var stop = new AtomicBoolean();
    try (var executor = Executors.newFixedThreadPool(numThreads)) {
      for (int i = 0; i < numThreads; i++) {
        executor.execute(
            () -> {
              while (!stop.get()) {
                cache.put(
                    "repo", SimpleTestObj.builder().id(idGen.incrementAndGet()).text(str).build());
                Thread.yield();
              }
            });
      }

      for (int i = 0; i < 50; i++) {
        Thread.sleep(10);
        var w = cache.currentWeightReported();
        if (w > admitWeight) {
          seenAdmittedWeightExceeded = true;
        }
      }

      stop.set(true);
    }

    // We may (with an low probability) see rejections.
    // Rejections are expected, but neither their occurrence nor their non-occurrence can be in any
    // way guaranteed by this test.
    // This means, assertions on the number of rejections and derived values are pretty much
    // impossible.
    // The probabilities are directly related to the system and state of that system running the
    // test.
    //
    // assertThat(cache.rejections()).isGreaterThan(0L);
    // assertThat(meterRejectedWeight.totalAmount()).isGreaterThan(0d);

    // This must actually never fail. (Those might still though, in very rare cases.)
    assertThat(cache.currentWeightReported()).isLessThanOrEqualTo(admitWeight);
    assertThat(seenAdmittedWeightExceeded).isFalse();
  }
}
