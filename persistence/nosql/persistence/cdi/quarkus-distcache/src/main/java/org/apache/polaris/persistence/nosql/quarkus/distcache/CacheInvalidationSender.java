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

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Collections.emptyList;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference.cacheInvalidationEvictReference;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.cacheInvalidations;
import static org.apache.polaris.persistence.nosql.quarkus.distcache.CacheInvalidationReceiver.CACHE_INVALIDATION_TOKEN_HEADER;
import static org.apache.polaris.persistence.nosql.quarkus.distcache.QuarkusDistributedCacheInvalidationsConfig.CACHE_INVALIDATIONS_CONFIG_PREFIX;
import static org.apache.polaris.persistence.nosql.quarkus.distcache.QuarkusDistributedCacheInvalidationsConfig.CONFIG_SERVICE_NAMES;
import static org.apache.polaris.persistence.nosql.quarkus.distcache.QuarkusDistributedCacheInvalidationsConfig.CONFIG_VALID_TOKENS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.quarkus.runtime.Startup;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidation;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Startup
class CacheInvalidationSender implements DistributedCacheInvalidation.Sender {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidationSender.class);

  private final Vertx vertx;
  private final long serviceNameLookupIntervalMillis;

  private final HttpClient httpClient;
  private final AddressResolver addressResolver;

  private final List<String> serviceNames;
  private final int httpPort;
  private final String invalidationUri;
  private final long requestTimeout;

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Lock lock = new ReentrantLock();
  private final int batchSize;
  private final BlockingQueue<CacheInvalidation> invalidations = new LinkedBlockingQueue<>();
  private boolean triggered;
  private final String token;

  /** Contains the IPv4/6 addresses resolved from {@link #serviceNames}. */
  private volatile List<String> resolvedAddresses = emptyList();

  @Inject
  CacheInvalidationSender(
      @SuppressWarnings("CdiInjectionPointsInspection") Vertx vertx,
      QuarkusDistributedCacheInvalidationsConfig config,
      @ConfigProperty(name = "quarkus.management.port") int httpPort,
      ServerInstanceId serverInstanceId) {
    this.vertx = vertx;

    this.addressResolver = new AddressResolver(vertx);
    this.requestTimeout =
        config
            .cacheInvalidationRequestTimeout()
            .orElse(Duration.of(30, ChronoUnit.SECONDS))
            .toMillis();
    this.httpClient = vertx.createHttpClient();
    this.serviceNames = config.cacheInvalidationServiceNames().orElse(emptyList());
    this.httpPort = httpPort;
    this.invalidationUri =
        config.cacheInvalidationUri() + "?sender=" + serverInstanceId.instanceId();
    this.serviceNameLookupIntervalMillis =
        config.cacheInvalidationServiceNameLookupInterval().toMillis();
    this.batchSize = config.cacheInvalidationBatchSize();
    this.token = config.cacheInvalidationValidTokens().map(List::getFirst).orElse(null);
    if (!serviceNames.isEmpty()) {
      try {
        LOGGER.info("Sending remote cache invalidations to service name(s) {}", serviceNames);
        updateServiceNames().toCompletionStage().toCompletableFuture().get();
        if (config.cacheInvalidationValidTokens().isEmpty()) {
          LOGGER.warn(
              "No token configured for cache invalidation messages - will not send any invalidation message. You need to configure the token(s) via {}.{}",
              CACHE_INVALIDATIONS_CONFIG_PREFIX,
              CONFIG_VALID_TOKENS);
        }
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to resolve service names " + serviceNames + " for remote cache invalidations",
            (e instanceof ExecutionException) ? e.getCause() : e);
      }
    } else if (token != null) {
      LOGGER.warn(
          "No service names are configured to send cache invalidation messages to - will not send any invalidation message. You need to configure the service name(s) via {}.{}",
          CACHE_INVALIDATIONS_CONFIG_PREFIX,
          CONFIG_SERVICE_NAMES);
    }
  }

  private Future<List<String>> updateServiceNames() {
    var previous = new HashSet<>(resolvedAddresses);
    return resolveServiceNames(serviceNames)
        .map(
            all ->
                all.stream().filter(adr -> !AddressResolver.LOCAL_ADDRESSES.contains(adr)).toList())
        .onSuccess(
            all -> {
              // refresh addresses regularly
              scheduleServiceNameResolution();

              var resolved = new HashSet<>(all);
              if (!resolved.equals(previous)) {
                LOGGER.info(
                    "Service names for remote cache invalidations {} now resolve to {}",
                    serviceNames,
                    all);
              }

              updateResolvedAddresses(all);
            })
        .onFailure(
            t -> {
              // refresh addresses regularly
              scheduleServiceNameResolution();

              LOGGER.warn("Failed to resolve service names: {}", t.toString());
            });
  }

  @VisibleForTesting
  void updateResolvedAddresses(List<String> all) {
    resolvedAddresses = all;
  }

  private void scheduleServiceNameResolution() {
    vertx.setTimer(serviceNameLookupIntervalMillis, x -> updateServiceNames());
  }

  @VisibleForTesting
  Future<List<String>> resolveServiceNames(List<String> serviceNames) {
    return addressResolver.resolveAll(serviceNames);
  }

  void enqueue(CacheInvalidation invalidation) {
    if (serviceNames.isEmpty() || token == null) {
      // Don't do anything if there are no targets to send invalidations to or whether no token has
      // been configured.
      return;
    }

    lock.lock();
    try {
      invalidations.add(invalidation);

      if (!triggered) {
        LOGGER.trace("Triggered invalidation submission");
        vertx.executeBlocking(this::sendInvalidations);
        triggered = true;
      }
    } finally {
      lock.unlock();
    }
  }

  private Void sendInvalidations() {
    var batch = new ArrayList<CacheInvalidation>(batchSize);
    try {
      while (true) {
        lock.lock();
        try {
          invalidations.drainTo(batch, 100);
          if (batch.isEmpty()) {
            LOGGER.trace("Done sending invalidations");
            triggered = false;
            break;
          }
        } finally {
          lock.unlock();
        }
        submit(batch, resolvedAddresses);
        batch = new ArrayList<>(batchSize);
      }
    } finally {
      // Handle the very unlikely case that the call to submit() failed and we cannot be sure that
      // the current batch was submitted.
      if (!batch.isEmpty()) {
        lock.lock();
        try {
          invalidations.addAll(batch);
          triggered = false;
        } finally {
          lock.unlock();
        }
      }
    }
    return null;
  }

  @VisibleForTesting
  List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
      List<CacheInvalidation> batch, List<String> resolvedAddresses) {
    LOGGER.trace("Submitting {} invalidations", batch.size());

    String json;
    try {
      json = objectMapper.writeValueAsString(cacheInvalidations(batch));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    var futures =
        new ArrayList<Future<Map.Entry<HttpClientResponse, Buffer>>>(resolvedAddresses.size());
    for (var address : resolvedAddresses) {
      futures.add(
          httpClient
              .request(HttpMethod.POST, httpPort, address, invalidationUri)
              .compose(
                  req ->
                      req.putHeader("Content-Type", APPLICATION_JSON)
                          .putHeader(CACHE_INVALIDATION_TOKEN_HEADER, token)
                          .send(json))
              .compose(resp -> resp.body().map(b -> Map.entry(resp, b)))
              .timeout(requestTimeout, TimeUnit.MILLISECONDS)
              .onComplete(
                  success -> {
                    var resp = success.getKey();
                    var statusCode = resp.statusCode();
                    if (statusCode != 200 && statusCode != 204) {
                      LOGGER.warn(
                          "{} cache invalidations could not be sent to {}:{}{} - HTTP {}/{} - body: {}",
                          batch.size(),
                          address,
                          httpPort,
                          invalidationUri,
                          statusCode,
                          resp.statusMessage(),
                          success.getValue());
                    } else {
                      LOGGER.trace(
                          "{} cache invalidations sent to {}:{}", batch.size(), address, httpPort);
                    }
                  },
                  failure -> {
                    if (failure instanceof SocketException
                        || failure instanceof UnknownHostException) {
                      LOGGER.warn(
                          "Technical network issue sending cache invalidations to {}:{}{} : {}",
                          address,
                          httpPort,
                          invalidationUri,
                          failure.getMessage());
                    } else {
                      LOGGER.error(
                          "Technical failure sending cache invalidations to {}:{}{}",
                          address,
                          httpPort,
                          invalidationUri,
                          failure);
                    }
                  }));
    }
    return futures;
  }

  @Override
  public void evictReference(@Nonnull String repositoryId, @Nonnull String refName) {
    enqueue(cacheInvalidationEvictReference(repositoryId, refName));
  }

  @Override
  public void evictObj(@Nonnull String repositoryId, @Nonnull ObjRef objId) {
    enqueue(cacheInvalidationEvictObj(repositoryId, objId));
  }
}
