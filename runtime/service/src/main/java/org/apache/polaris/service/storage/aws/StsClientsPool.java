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

package org.apache.polaris.service.storage.aws;

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

/** Maintains a pool of STS clients. */
public class StsClientsPool implements StsClientProvider {
  // CODE_COPIED_TO_POLARIS from Project Nessie 0.104.2

  private static final String CACHE_NAME = "sts-clients";

  private final Cache<StsDestination, StsClient> clients;
  private final Function<StsDestination, StsClient> clientBuilder;

  public StsClientsPool(
      int clientsCacheMaxSize, SdkHttpClient sdkHttpClient, MeterRegistry meterRegistry) {
    this(
        clientsCacheMaxSize,
        key -> defaultStsClient(key, sdkHttpClient),
        Optional.ofNullable(meterRegistry));
  }

  @VisibleForTesting
  StsClientsPool(
      int maxSize,
      Function<StsDestination, StsClient> clientBuilder,
      Optional<MeterRegistry> meterRegistry) {
    this.clientBuilder = clientBuilder;
    this.clients =
        Caffeine.newBuilder()
            .maximumSize(maxSize)
            .recordStats(() -> statsCounter(meterRegistry, maxSize))
            .build();
  }

  @Override
  public StsClient stsClient(StsDestination destination) {
    return clients.get(destination, clientBuilder);
  }

  private static StsClient defaultStsClient(StsDestination parameters, SdkHttpClient sdkClient) {
    StsClientBuilder builder = StsClient.builder();
    builder.httpClient(sdkClient);
    if (parameters.endpoint().isPresent()) {
      CompletableFuture<Endpoint> endpointFuture =
          completedFuture(Endpoint.builder().url(parameters.endpoint().get()).build());
      builder.endpointProvider(params -> endpointFuture);
    }

    parameters.region().ifPresent(r -> builder.region(Region.of(r)));

    return builder.build();
  }

  static StatsCounter statsCounter(Optional<MeterRegistry> meterRegistry, int maxSize) {
    if (meterRegistry.isPresent()) {
      meterRegistry
          .get()
          .gauge("max_entries", singletonList(Tag.of("cache", CACHE_NAME)), "", x -> maxSize);

      return new CaffeineStatsCounter(meterRegistry.get(), CACHE_NAME);
    }
    return StatsCounter.disabledStatsCounter();
  }
}
