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

import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.context.RealmContextResolver;

@ApplicationScoped
public class RealmIdTagContributor implements HttpServerMetricsTagsContributor {

  public static final String TAG_REALM = "realm_id";

  private static final Tags UNFINISHED_RESOLUTION_TAGS = Tags.of(TAG_REALM, "???");

  @Inject QuarkusMetricsConfiguration metricsConfiguration;
  @Inject RealmContextResolver realmContextResolver;

  @Override
  public Tags contribute(Context context) {
    // FIXME retrieve the realm context from context.requestContextLocalData()
    // after upgrading to Quarkus 3.24
    if (!metricsConfiguration.realmIdTag().enableInHttpMetrics()) {
      return Tags.empty();
    }
    HttpServerRequest request = context.request();
    try {
      return realmContextResolver
          .resolveRealmContext(
              request.absoluteURI(),
              request.method().name(),
              request.path(),
              request.headers()::get)
          .thenApply(this::successfulResolutionTags)
          .toCompletableFuture()
          // get the result of the CompletableFuture if it's already completed,
          // otherwise return UNFINISHED_RESOLUTION_TAGS as this code is executed on
          // an event loop thread, and we don't want to block it.
          .getNow(UNFINISHED_RESOLUTION_TAGS);
    } catch (Exception e) {
      return Tags.empty();
    }
  }

  private Tags successfulResolutionTags(RealmContext realmContext) {
    return Tags.of(TAG_REALM, realmContext.getRealmIdentifier());
  }
}
