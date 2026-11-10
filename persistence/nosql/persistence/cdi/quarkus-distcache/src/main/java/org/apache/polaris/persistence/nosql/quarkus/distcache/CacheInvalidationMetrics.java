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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidation;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference;

/** Metrics shared by the sender and receiver, with a fixed set of label values. */
@ApplicationScoped
class CacheInvalidationMetrics {
  static final String PREFIX = "polaris.cache.invalidation.";

  enum SkipReason {
    NO_DISCOVERY,
    NO_TOKEN,
    NO_PEERS
  }

  enum SendOutcome {
    SUCCESS,
    TIMEOUT,
    TRANSPORT_ERROR,
    HTTP_REJECTED
  }

  enum ReceiveOutcome {
    PROCESSED,
    SELF,
    INVALID_TOKEN,
    INVALID_CONTENT_TYPE,
    INVALID_PAYLOAD,
    HANDLER_ERROR
  }

  enum DiscoveryOutcome {
    SUCCESS,
    FAILURE
  }

  private final MeterRegistry registry;
  private final Map<String, Counter> enqueued;
  private final Map<String, Counter> dispatched;
  private final Map<SkipReason, Counter> skipped = new EnumMap<>(SkipReason.class);
  private final Map<SendOutcome, Timer> sends;
  private final Map<ReceiveOutcome, Timer> receives;
  private final Map<DiscoveryOutcome, Timer> discoveries;
  private final DistributionSummary batchSize;
  private final AtomicInteger inflight = new AtomicInteger();
  private final AtomicInteger peers = new AtomicInteger();
  private final AtomicLong lastDiscoverySuccess = new AtomicLong();

  @Inject
  CacheInvalidationMetrics(@Any Instance<MeterRegistry> registries) {
    this(registries.stream().findAny());
  }

  CacheInvalidationMetrics(Optional<MeterRegistry> registry) {
    // An empty composite supplies no-op meters when metrics are not available.
    this.registry = registry.orElseGet(CompositeMeterRegistry::new);
    enqueued = entryCounters("enqueued");
    dispatched = entryCounters("dispatched");
    for (var reason : SkipReason.values()) {
      skipped.put(
          reason,
          Counter.builder(PREFIX + "skipped")
              .description("Invalidation entries skipped before any delivery attempt.")
              .tag("reason", label(reason))
              .register(this.registry));
    }
    sends =
        timers("send.duration", "Duration of one peer HTTP delivery attempt.", SendOutcome.class);
    receives =
        timers(
            "receive.duration",
            "Duration of handling one invalidation request.",
            ReceiveOutcome.class);
    discoveries =
        timers(
            "discovery.duration",
            "Duration of one peer discovery attempt.",
            DiscoveryOutcome.class);
    batchSize =
        DistributionSummary.builder(PREFIX + "batch.size")
            .description("Invalidation entries per batch before fan-out to peers.")
            .register(this.registry);
    Gauge.builder(PREFIX + "send.inflight", inflight, AtomicInteger::get)
        .description("Outstanding peer HTTP delivery attempts.")
        .register(this.registry);
    Gauge.builder(PREFIX + "discovery.peers", peers, AtomicInteger::get)
        .description(
            "Usable peers from the last successful discovery, after local-address filtering.")
        .register(this.registry);
    Gauge.builder(
            PREFIX + "discovery.last_success.timestamp",
            lastDiscoverySuccess,
            value -> value.get() / 1000.0)
        .description("Unix timestamp in seconds of the last successful discovery, or zero if none.")
        .baseUnit("seconds")
        .register(this.registry);
  }

  void registerQueue(Queue<CacheInvalidation> queue) {
    Gauge.builder(PREFIX + "queue.depth", queue, Queue::size)
        .description("Invalidation entries waiting to be submitted, excluding in-flight requests.")
        .register(registry);
  }

  private Map<String, Counter> entryCounters(String stage) {
    return Map.of(
        CacheInvalidationEvictObj.TYPE, entryCounter(stage, CacheInvalidationEvictObj.TYPE),
        CacheInvalidationEvictReference.TYPE,
            entryCounter(stage, CacheInvalidationEvictReference.TYPE));
  }

  private Counter entryCounter(String stage, String type) {
    return Counter.builder(PREFIX + "entries")
        .description(
            "Invalidation entries enqueued locally or successfully dispatched to local cache handlers.")
        .tags("stage", stage, "type", type)
        .register(registry);
  }

  private <E extends Enum<E>> Map<E, Timer> timers(
      String suffix, String description, Class<E> outcomes) {
    var timers = new EnumMap<E, Timer>(outcomes);
    for (var outcome : outcomes.getEnumConstants()) {
      timers.put(
          outcome,
          Timer.builder(PREFIX + suffix)
              .description(description)
              .tag("outcome", label(outcome))
              .register(registry));
    }
    return timers;
  }

  private static String label(Enum<?> value) {
    return value.name().toLowerCase(Locale.ROOT);
  }

  void enqueued(CacheInvalidation invalidation) {
    countEntry(enqueued, invalidation);
  }

  void dispatched(CacheInvalidation invalidation) {
    countEntry(dispatched, invalidation);
  }

  private static void countEntry(Map<String, Counter> counters, CacheInvalidation invalidation) {
    var counter = counters.get(invalidation.type());
    if (counter != null) {
      counter.increment();
    }
  }

  void skipped(SkipReason reason, int entries) {
    skipped.get(reason).increment(entries);
  }

  void batch(int entries) {
    batchSize.record(entries);
  }

  Timer.Sample start() {
    return Timer.start(registry);
  }

  Timer.Sample sendStarted() {
    inflight.incrementAndGet();
    return start();
  }

  void sendCompleted(Timer.Sample sample, SendOutcome outcome) {
    inflight.decrementAndGet();
    sample.stop(sends.get(outcome));
  }

  void receiveCompleted(Timer.Sample sample, ReceiveOutcome outcome) {
    sample.stop(receives.get(outcome));
  }

  void discoveryCompleted(Timer.Sample sample, DiscoveryOutcome outcome) {
    if (outcome == DiscoveryOutcome.SUCCESS) {
      lastDiscoverySuccess.set(registry.config().clock().wallTime());
    }
    sample.stop(discoveries.get(outcome));
  }

  void peers(int count) {
    peers.set(count);
  }
}
