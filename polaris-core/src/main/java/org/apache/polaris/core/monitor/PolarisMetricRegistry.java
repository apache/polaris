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
import io.micrometer.core.instrument.Timer;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.core.resource.TimedApi;

/**
 * Wrapper around the Micrometer {@link MeterRegistry} providing additional metric management
 * functions for the Polaris application. Implements in-memory caching of timers and counters.
 * Records two metrics for each instrument with one tagged by the realm ID (realm-specific metric)
 * and one without. The realm-specific metric is suffixed with ".realm".
 */
public class PolarisMetricRegistry {
  private final MeterRegistry meterRegistry;
  private final ConcurrentMap<String, Timer> timers = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();
  private static final String TAG_REALM = "REALM_ID";
  private static final String TAG_RESP_CODE = "HTTP_RESPONSE_CODE";
  private static final String SUFFIX_COUNTER = ".count";
  private static final String SUFFIX_ERROR = ".error";
  private static final String SUFFIX_REALM = ".realm";

  public PolarisMetricRegistry(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public MeterRegistry getMeterRegistry() {
    return meterRegistry;
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
          Counter.builder(metric + SUFFIX_ERROR).tags(TAG_RESP_CODE, "400").register(meterRegistry);
          Counter.builder(metric + SUFFIX_ERROR).tags(TAG_RESP_CODE, "500").register(meterRegistry);
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
                    .register(meterRegistry));
    timerRealm.record(elapsedTimeMs, TimeUnit.MILLISECONDS);
  }

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
                    .register(meterRegistry));
    counterRealm.increment();
  }

  public void incrementErrorCounter(String metric, int statusCode, String realmId) {
    String errorMetric = metric + SUFFIX_ERROR;
    Counter.builder(errorMetric)
        .tag(TAG_RESP_CODE, String.valueOf(statusCode))
        .register(meterRegistry)
        .increment();

    Counter.builder(errorMetric + SUFFIX_REALM)
        .tag(TAG_RESP_CODE, String.valueOf(statusCode))
        .tag(TAG_REALM, realmId)
        .register(meterRegistry)
        .increment();
  }
}
