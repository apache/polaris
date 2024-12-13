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
package org.apache.polaris.service.dropwizard.tracing;

import io.opentelemetry.api.trace.Span;
import io.quarkus.vertx.web.RouteFilter;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.service.dropwizard.logging.QuarkusLoggingMDCFilter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class QuarkusTracingFilter {

  public static final String REQUEST_ID_ATTRIBUTE = "polaris.request.id";
  public static final String REALM_ID_ATTRIBUTE = "polaris.realm";

  @ConfigProperty(name = "quarkus.otel.sdk.disabled")
  boolean sdkDisabled;

  @RouteFilter(QuarkusLoggingMDCFilter.PRIORITY - 1)
  public void applySpanAttributes(RoutingContext rc) {
    if (!sdkDisabled) {
      Span span = Span.current();
      String requestId = QuarkusLoggingMDCFilter.requestId(rc);
      String realmId = QuarkusLoggingMDCFilter.realmId(rc);
      if (requestId != null) {
        span.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);
      }
      span.setAttribute(REALM_ID_ATTRIBUTE, realmId);
    }
    rc.next();
  }
}
