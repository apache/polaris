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

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.util.Collections.emptyList;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.http.ManagementInterface;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// See https://quarkus.io/guides/management-interface-reference#management-endpoint-application
@ApplicationScoped
class CacheInvalidationReceiver {
  static final String CACHE_INVALIDATION_TOKEN_HEADER = "Polaris-Cache-Invalidation-Token";

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidationReceiver.class);

  private final DistributedCacheInvalidation distributedCacheInvalidation;
  private final String serverInstanceId;
  private final Set<String> validTokens;
  private final String invalidationPath;
  private final ObjectMapper objectMapper;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  CacheInvalidationReceiver(
      QuarkusDistributedCacheInvalidationsConfig storeConfig,
      ServerInstanceId serverInstanceId,
      DistributedCacheInvalidation.Receiver distributedCacheInvalidation) {
    this.distributedCacheInvalidation = distributedCacheInvalidation;
    this.serverInstanceId = serverInstanceId.instanceId();
    this.invalidationPath = storeConfig.cacheInvalidationUri();
    this.validTokens =
        new HashSet<>(storeConfig.cacheInvalidationValidTokens().orElse(emptyList()));
    this.objectMapper =
        new ObjectMapper()
            // forward compatibility
            .disable(FAIL_ON_UNKNOWN_PROPERTIES);
  }

  void registerManagementRoutes(@Observes ManagementInterface mi) {
    mi.router().post(invalidationPath).handler(this::cacheInvalidations);
  }

  void cacheInvalidations(RoutingContext rc) {
    var request = rc.request();
    var senderId = request.getParam("sender");
    var token = request.getHeader(CACHE_INVALIDATION_TOKEN_HEADER);

    cacheInvalidations(
        rc,
        () -> {
          try {
            var json = rc.body().asString();
            if (json == null || json.isEmpty()) {
              return CacheInvalidations.cacheInvalidations(emptyList());
            }
            return objectMapper.readValue(json, CacheInvalidations.class);
          } catch (Exception e) {
            LOGGER.error("Failed to deserialize cache invalidation", e);
            return CacheInvalidations.cacheInvalidations(emptyList());
          }
        },
        senderId,
        token);
  }

  void cacheInvalidations(
      RoutingContext rc,
      Supplier<CacheInvalidations> invalidations,
      String senderId,
      String token) {
    if (token == null || !validTokens.contains(token)) {
      LOGGER.warn("Received cache invalidation with invalid token {}", token);
      responseInvalidToken(rc);
      return;
    }
    if (serverInstanceId.equals(senderId)) {
      LOGGER.trace("Ignoring invalidations from local instance");
      responseNoContent(rc);
      return;
    }
    if (!"application/json".equals(rc.request().getHeader("Content-Type"))) {
      LOGGER.warn("Received cache invalidation with invalid HTTP content type");
      responseInvalidContentType(rc);
      return;
    }

    List<CacheInvalidations.CacheInvalidation> invalidationList;
    try {
      invalidationList = invalidations.get().invalidations();
    } catch (RuntimeException e) {
      responseServerError(rc);
      return;
    }

    var cacheInvalidation = distributedCacheInvalidation;
    if (cacheInvalidation != null) {
      for (CacheInvalidations.CacheInvalidation invalidation : invalidationList) {
        switch (invalidation.type()) {
          case CacheInvalidationEvictObj.TYPE -> {
            var putObj = (CacheInvalidationEvictObj) invalidation;
            cacheInvalidation.evictObj(putObj.realmId(), putObj.id());
          }
          case CacheInvalidationEvictReference.TYPE -> {
            var putReference = (CacheInvalidationEvictReference) invalidation;
            cacheInvalidation.evictReference(putReference.realmId(), putReference.ref());
          }
          default -> {
            // nothing we can do about a new invalidation type here
          }
        }
      }
    }

    responseNoContent(rc);
  }

  private void responseServerError(RoutingContext rc) {
    rc.response().setStatusCode(500).setStatusMessage("Server error parsing request body").end();
  }

  private void responseInvalidToken(RoutingContext rc) {
    rc.response().setStatusCode(400).setStatusMessage("Invalid token").end();
  }

  private void responseInvalidContentType(RoutingContext rc) {
    rc.response().setStatusCode(415).setStatusMessage("Unsupported media type").end();
  }

  private void responseNoContent(RoutingContext rc) {
    rc.response().setStatusCode(204).setStatusMessage("No content").end();
  }
}
