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
package org.apache.polaris.service.quarkus.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.internal.OnlyOnceLoggingDenyMeterFilter;
import io.quarkus.micrometer.runtime.binder.HttpBinderConfiguration;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.stream.Collectors;

public class QuarkusMeterFilterProducer {

  @Inject QuarkusMetricsConfiguration metricsConfiguration;
  @Inject HttpBinderConfiguration binderConfiguration;

  @Produces
  @Singleton
  public MeterFilter commonTagsFilter() {
    return MeterFilter.commonTags(
        this.metricsConfiguration.tags().entrySet().stream()
            .map(e -> Tag.of(e.getKey(), e.getValue()))
            .collect(Collectors.toSet()));
  }

  @Produces
  @Singleton
  public MeterFilter maxRealmIdTagsInHttpMetricsFilter() {
    MeterFilter denyFilter =
        new OnlyOnceLoggingDenyMeterFilter(
            () ->
                String.format(
                    "Reached the maximum number (%s) of '%s' tags for '%s'",
                    metricsConfiguration.realmIdTag().httpMetricsMaxCardinality(),
                    RealmIdTagContributor.TAG_REALM,
                    binderConfiguration.getHttpServerRequestsName()));
    return MeterFilter.maximumAllowableTags(
        binderConfiguration.getHttpServerRequestsName(),
        RealmIdTagContributor.TAG_REALM,
        metricsConfiguration.realmIdTag().httpMetricsMaxCardinality(),
        denyFilter);
  }
}
