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
package org.apache.polaris.service.quarkus.tracing;

import io.opentelemetry.api.trace.Span;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.quarkus.config.QuarkusFilterPriorities;
import org.apache.polaris.service.quarkus.context.RealmContextFilter;
import org.apache.polaris.service.quarkus.logging.QuarkusLoggingMDCFilter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@PreMatching
@ApplicationScoped
@Priority(QuarkusFilterPriorities.TRACING_FILTER)
@Provider
public class QuarkusTracingFilter implements ContainerRequestFilter {

  public static final String REQUEST_ID_ATTRIBUTE = "polaris.request.id";
  public static final String REALM_ID_ATTRIBUTE = "polaris.realm.id";

  @ConfigProperty(name = "quarkus.otel.sdk.disabled")
  boolean sdkDisabled;

  @Override
  public void filter(ContainerRequestContext rc) {
    if (!sdkDisabled) {
      Span span = Span.current();
      String requestId = (String) rc.getProperty(QuarkusLoggingMDCFilter.REQUEST_ID_KEY);
      if (requestId != null) {
        span.setAttribute(REQUEST_ID_ATTRIBUTE, requestId);
      }
      RealmContext realmContext =
          (RealmContext) rc.getProperty(RealmContextFilter.REALM_CONTEXT_KEY);
      span.setAttribute(REALM_ID_ATTRIBUTE, realmContext.getRealmIdentifier());
    }
  }
}
