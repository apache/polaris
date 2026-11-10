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
package org.apache.polaris.persistence.nosql.quarkus.distcache;

import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference.cacheInvalidationEvictReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.apache.polaris.persistence.nosql.quarkus.distcache.CacheInvalidationMetrics.DiscoveryOutcome;
import org.apache.polaris.persistence.nosql.quarkus.distcache.CacheInvalidationMetrics.SendOutcome;
import org.junit.jupiter.api.Test;

class TestCacheInvalidationMetrics {
  @Test
  void fixedCardinalityAndNoHistogramsByDefault() {
    var registry = new SimpleMeterRegistry();
    try {
      var metrics = new CacheInvalidationMetrics(Optional.of(registry));
      metrics.registerQueue(new LinkedBlockingQueue<>());
      for (var i = 0; i < 100; i++) {
        metrics.enqueued(cacheInvalidationEvictObj("realm-" + i, ObjRef.objRef("object-" + i, i)));
        metrics.dispatched(cacheInvalidationEvictReference("realm-" + i, "reference-" + i));
      }
      assertThat(registry.getMeters()).hasSize(24);
      assertThat(registry.getMeters().stream().map(meter -> meter.getId().getName()).distinct())
          .hasSize(10);
      assertThat(registry.getMeters())
          .allSatisfy(
              meter -> {
                assertThat(meter.getId().getTags())
                    .allSatisfy(
                        tag -> {
                          assertThat(tag.getKey()).isIn("stage", "type", "reason", "outcome");
                          assertThat(tag.getValue())
                              .doesNotContain("realm-", "object-", "reference-");
                        });
                if (meter instanceof Timer timer) {
                  assertThat(timer.takeSnapshot().histogramCounts()).isEmpty();
                  assertThat(timer.takeSnapshot().percentileValues()).isEmpty();
                } else if (meter instanceof DistributionSummary summary) {
                  assertThat(summary.takeSnapshot().histogramCounts()).isEmpty();
                  assertThat(summary.takeSnapshot().percentileValues()).isEmpty();
                }
              });
    } finally {
      registry.close();
    }
  }

  @Test
  void discoveryFailureRetainsLastSuccessAndPeerCount() {
    var clock = new MockClock();
    var registry = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
    try {
      var metrics = new CacheInvalidationMetrics(Optional.of(registry));
      var timestamp =
          registry
              .get(CacheInvalidationMetrics.PREFIX + "discovery.last_success.timestamp")
              .gauge();
      assertThat(timestamp.value()).isZero();
      var success = metrics.start();
      clock.add(Duration.ofMillis(10));
      metrics.peers(3);
      metrics.discoveryCompleted(success, DiscoveryOutcome.SUCCESS);
      var lastSuccess = timestamp.value();
      assertThat(lastSuccess).isEqualTo(clock.wallTime() / 1000.0);
      var failure = metrics.start();
      clock.add(Duration.ofSeconds(2));
      metrics.discoveryCompleted(failure, DiscoveryOutcome.FAILURE);
      assertThat(timestamp.value()).isEqualTo(lastSuccess);
      assertThat(registry.get(CacheInvalidationMetrics.PREFIX + "discovery.peers").gauge().value())
          .isEqualTo(3);
      assertThat(
              registry
                  .get(CacheInvalidationMetrics.PREFIX + "discovery.duration")
                  .tag("outcome", "failure")
                  .timer()
                  .totalTime(TimeUnit.SECONDS))
          .isEqualTo(2);
    } finally {
      registry.close();
    }
  }

  @Test
  void metricsAreOptional() {
    assertThatCode(
            () -> {
              var metrics = new CacheInvalidationMetrics(Optional.empty());
              metrics.registerQueue(new LinkedBlockingQueue<CacheInvalidation>());
              metrics.enqueued(cacheInvalidationEvictReference("realm", "reference"));
              metrics.sendCompleted(metrics.sendStarted(), SendOutcome.SUCCESS);
              metrics.discoveryCompleted(metrics.start(), DiscoveryOutcome.SUCCESS);
            })
        .doesNotThrowAnyException();
  }
}
