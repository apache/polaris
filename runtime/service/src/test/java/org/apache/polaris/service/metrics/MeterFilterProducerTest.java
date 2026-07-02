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
package org.apache.polaris.service.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.quarkus.micrometer.runtime.binder.HttpBinderConfiguration;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MeterFilterProducerTest {

  private static final String HTTP_SERVER_REQUESTS = "http.server.requests";

  @Test
  void noSlosProducesNoOpFilter() {
    MeterFilter filter = newProducer(Optional.empty()).httpServerRequestsHistogramFilter();
    DistributionStatisticConfig input = DistributionStatisticConfig.DEFAULT;
    DistributionStatisticConfig output = filter.configure(timerId(HTTP_SERVER_REQUESTS), input);
    assertThat(output).isSameAs(input);
  }

  @Test
  void slosAppliedToMatchingTimer() {
    List<Duration> slos =
        List.of(Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofSeconds(1));
    MeterFilter filter = newProducer(Optional.of(slos)).httpServerRequestsHistogramFilter();
    DistributionStatisticConfig output =
        filter.configure(timerId(HTTP_SERVER_REQUESTS), DistributionStatisticConfig.DEFAULT);
    assertThat(output.getServiceLevelObjectiveBoundaries())
        .containsExactly(
            (double) Duration.ofMillis(10).toNanos(),
            (double) Duration.ofMillis(100).toNanos(),
            (double) Duration.ofSeconds(1).toNanos());
  }

  @Test
  void slosNotAppliedToOtherTimer() {
    MeterFilter filter =
        newProducer(Optional.of(List.of(Duration.ofMillis(10))))
            .httpServerRequestsHistogramFilter();
    DistributionStatisticConfig input = DistributionStatisticConfig.DEFAULT;
    DistributionStatisticConfig output = filter.configure(timerId("some.other.timer"), input);
    assertThat(output).isSameAs(input);
  }

  @Test
  void slosNotAppliedToNonTimerMeter() {
    MeterFilter filter =
        newProducer(Optional.of(List.of(Duration.ofMillis(10))))
            .httpServerRequestsHistogramFilter();
    DistributionStatisticConfig input = DistributionStatisticConfig.DEFAULT;
    Meter.Id counter =
        new Meter.Id(HTTP_SERVER_REQUESTS, Tags.empty(), null, null, Meter.Type.COUNTER);
    DistributionStatisticConfig output = filter.configure(counter, input);
    assertThat(output).isSameAs(input);
  }

  private static MeterFilterProducer newProducer(Optional<List<Duration>> slos) {
    MetricsConfiguration.HttpServerRequests httpServerRequests =
        mock(MetricsConfiguration.HttpServerRequests.class);
    when(httpServerRequests.histogramSlos()).thenReturn(slos);
    MetricsConfiguration metricsConfiguration = mock(MetricsConfiguration.class);
    when(metricsConfiguration.httpServerRequests()).thenReturn(httpServerRequests);
    HttpBinderConfiguration binderConfiguration = mock(HttpBinderConfiguration.class);
    when(binderConfiguration.getHttpServerRequestsName()).thenReturn(HTTP_SERVER_REQUESTS);
    MeterFilterProducer producer = new MeterFilterProducer();
    producer.metricsConfiguration = metricsConfiguration;
    producer.binderConfiguration = binderConfiguration;
    return producer;
  }

  private static Meter.Id timerId(String name) {
    return new Meter.Id(name, Tags.empty(), null, null, Meter.Type.TIMER);
  }
}
