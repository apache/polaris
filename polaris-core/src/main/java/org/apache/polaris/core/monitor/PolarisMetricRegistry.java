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
package org.apache.polaris.core.monitor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.resource.TimedApi;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Wrapper around the Micrometer {@link MeterRegistry} providing additional metric management
 * functions for the Polaris application. Implements in-memory caching of timers and counters.
 * Records two metrics for each instrument with one tagged by the realm ID (realm-specific metric)
 * and one without. The realm-specific metric is suffixed with ".realm".
 *
 * <p>Uppercase tag names are now deprecated. Prefer snake_casing instead. Old metrics are emitted
 * with both variations but the uppercase versions may eventually be removed. New methods for tag
 * emission (those that allow you to pass in an Iterable<Tag>) only emit the snake_case version.
 */
public class PolarisMetricRegistry {
  private final MeterRegistry meterRegistry;
  private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();

  /**
   * @deprecated See class Javadoc.
   */
  public static final String TAG_REALM_DEPRECATED = "REALM_ID";

  public static final String TAG_REALM = "realm_id";

  /**
   * @deprecated See class Javadoc.
   */
  public static final String TAG_RESP_CODE_DEPRECATED = "HTTP_RESPONSE_CODE";

  public static final String TAG_RESP_CODE = "http_response_code";

  public static final String SUFFIX_COUNTER = ".count";
  public static final String SUFFIX_ERROR = ".error";
  public static final String SUFFIX_REALM = ".realm";

  public PolarisMetricRegistry(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
    new ClassLoaderMetrics().bindTo(meterRegistry);
    new JvmMemoryMetrics().bindTo(meterRegistry);
    new JvmGcMetrics().bindTo(meterRegistry);
    new ProcessorMetrics().bindTo(meterRegistry);
    new JvmThreadMetrics().bindTo(meterRegistry);
  }

  public MeterRegistry getMeterRegistry() {
    return meterRegistry;
  }

  @VisibleForTesting
  public void clear() {
    meterRegistry.clear();
    counters.clear();
  }

  public void init(Class<?>... classes) {
    for (Class<?> clazz : classes) {
      Method[] methods = clazz.getDeclaredMethods();
      for (Method method : methods) {
        if (method.isAnnotationPresent(TimedApi.class)) {
          TimedApi timedApi = method.getAnnotation(TimedApi.class);
          String metric = timedApi.value();
          timers.put(metric, Timer.builder(metric).register(meterRegistry));
          counters.put(
              metric + SUFFIX_COUNTER,
              Counter.builder(metric + SUFFIX_COUNTER).register(meterRegistry));

          // Error counters contain the HTTP response code in a tag, thus caching them would not be
          // meaningful.
          Counter.builder(metric + SUFFIX_ERROR)
              .tags(TAG_RESP_CODE, "400", TAG_RESP_CODE_DEPRECATED, "400")
              .register(meterRegistry);
          Counter.builder(metric + SUFFIX_ERROR)
              .tags(TAG_RESP_CODE, "500", TAG_RESP_CODE_DEPRECATED, "500")
              .register(meterRegistry);
        }
      }
    }
  }

  public void recordTimer(String metric, long elapsedTimeMs, String realmId) {
    Timer timer =
        timers.computeIfAbsent(metric, m -> Timer.builder(metric).register(meterRegistry));
    timer.record(elapsedTimeMs, TimeUnit.MILLISECONDS);

    Timer timerRealm =
        timers.computeIfAbsent(
            metric + SUFFIX_REALM,
            m ->
                Timer.builder(metric + SUFFIX_REALM)
                    .tag(TAG_REALM, realmId)
                    .tag(TAG_REALM_DEPRECATED, realmId)
                    .register(meterRegistry));
    timerRealm.record(elapsedTimeMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Increments metric.count and metric.count.realm. The realmId is tagged on the latter metric.
   * Counters are cached.
   */
  public void incrementCounter(String metric, String realmId) {
    String counterMetric = metric + SUFFIX_COUNTER;
    Counter counter =
        counters.computeIfAbsent(
            counterMetric, m -> Counter.builder(counterMetric).register(meterRegistry));
    counter.increment();

    Counter counterRealm =
        counters.computeIfAbsent(
            counterMetric + SUFFIX_REALM,
            m ->
                Counter.builder(counterMetric + SUFFIX_REALM)
                    .tag(TAG_REALM, realmId)
                    .tag(TAG_REALM_DEPRECATED, realmId)
                    .register(meterRegistry));
    counterRealm.increment();
  }

  /**
   * Increments metric.error and metric.error.realm. The realmId is tagged on the latter metric.
   * Both metrics are tagged with the statusCode and counters are not cached.
   */
  public void incrementErrorCounter(String metric, int statusCode, String realmId) {
    String errorMetric = metric + SUFFIX_ERROR;
    Counter.builder(errorMetric)
        .tag(TAG_RESP_CODE, String.valueOf(statusCode))
        .tag(TAG_RESP_CODE_DEPRECATED, String.valueOf(statusCode))
        .register(meterRegistry)
        .increment();

    Counter.builder(errorMetric + SUFFIX_REALM)
        .tag(TAG_RESP_CODE, String.valueOf(statusCode))
        .tag(TAG_RESP_CODE_DEPRECATED, String.valueOf(statusCode))
        .tag(TAG_REALM, realmId)
        .tag(TAG_REALM_DEPRECATED, realmId)
        .register(meterRegistry)
        .increment();
  }

  /**
   * Increments metric.count and metric.count.realm. The realmId is tagged on the latter metric.
   * Arbitrary tags can be specified and counters are not cached.
   */
  public void incrementCounter(String metric, String realmId, Iterable<Tag> tags) {
    Counter.builder(metric + SUFFIX_COUNTER).tags(tags).register(meterRegistry).increment();

    Counter.builder(metric + SUFFIX_COUNTER + SUFFIX_REALM)
        .tags(tags)
        .tag(TAG_REALM, realmId)
        .register(meterRegistry)
        .increment();
  }

  /**
   * Increments metric.count and metric.count.realm. The realmId is tagged on the latter metric.
   * Arbitrary tags can be specified and counters are not cached.
   */
  public void incrementCounter(String metric, String realmId, Tag tag) {
    incrementCounter(metric, realmId, Collections.singleton(tag));
  }

  /**
   * Increments metric.error and metric.error.realm. The realmId is tagged on the latter metric.
   * Arbitrary tags can be specified and counters are not cached.
   */
  public void incrementErrorCounter(String metric, String realmId, Iterable<Tag> tags) {
    Counter.builder(metric + SUFFIX_ERROR).tags(tags).register(meterRegistry).increment();

    Counter.builder(metric + SUFFIX_ERROR + SUFFIX_REALM)
        .tags(tags)
        .tag(TAG_REALM, realmId)
        .register(meterRegistry)
        .increment();
  }

  /**
   * Increments metric.error and metric.error.realm. The realmId is tagged on the latter metric.
   * Arbitrary tags can be specified and counters are not cached.
   */
  public void incrementErrorCounter(String metric, String realmId, Tag tag) {
    incrementErrorCounter(metric, realmId, Collections.singleton(tag));
  }
}
