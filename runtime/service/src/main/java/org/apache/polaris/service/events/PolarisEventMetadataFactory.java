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
package org.apache.polaris.service.events;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.quarkus.security.identity.CurrentIdentityAssociation;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.jboss.resteasy.reactive.server.core.CurrentRequestManager;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;
import org.jboss.resteasy.reactive.server.jaxrs.ContainerRequestContextImpl;

@ApplicationScoped
public class PolarisEventMetadataFactory {

  @Inject Clock clock;
  @Inject CurrentIdentityAssociation currentIdentityAssociation;
  @Inject Instance<RealmContext> realmContext;

  /**
   * Creates a new event metadata object.
   *
   * <p>This method should only be called with the request scope active in production. It may be
   * called outside the request scope in tests, in which case some fields may be missing.
   */
  public PolarisEventMetadata create() {
    return PolarisEventMetadata.builder()
        .timestamp(clock.instant())
        .realmId(getRealmId())
        .user(getUser())
        .requestId(getRequestId())
        .openTelemetryContext(getOpenTelemetryContext())
        .build();
  }

  /**
   * Creates a copy of the given metadata, with a new timestamp and new OpenTelemetry context.
   *
   * <p>Contrary to {@link #create()}, this method does not require an active request scope, and is
   * safe to use from any thread, e.g. from a task executor thread.
   */
  public PolarisEventMetadata copy(PolarisEventMetadata original) {
    return PolarisEventMetadata.builder()
        .from(original)
        .timestamp(clock.instant())
        .openTelemetryContext(getOpenTelemetryContext())
        .build();
  }

  /**
   * Extracts the realm ID from the current realm context.
   *
   * <p>In the unlikely case there is no realm context, e.g. in some tests, returns
   * "UNKNOWN_REALM_ID".
   */
  private String getRealmId() {
    return realmContext.isResolvable()
        ? realmContext.get().getRealmIdentifier()
        : "UNKNOWN_REALM_ID";
  }

  /**
   * Extracts the user from the current security identity.
   *
   * <p>Note: we must avoid injecting {@link SecurityIdentity} here, because Polaris uses lazy
   * authentication and this method may be invoked at a moment where authentication has not yet
   * taken place, e.g. in pre-authentication filters.
   */
  private Optional<PolarisPrincipal> getUser() {
    SecurityIdentity identity =
        currentIdentityAssociation.getDeferredIdentity().subscribeAsCompletionStage().getNow(null);
    return identity == null || identity.isAnonymous()
        ? Optional.empty()
        : Optional.of(identity.getPrincipal(PolarisPrincipal.class));
  }

  /**
   * Extracts the request ID from the current request context.
   *
   * <p>Note: we must avoid injecting {@link jakarta.ws.rs.container.ContainerRequestContext} here,
   * because this may cause some tests to fail, e.g. when running with no active request scope.
   *
   * <p>Using {@code Instance<ContainerRequestContext>} injection doesn't work either, because it's
   * a special bean that always appears resolvable, even when it's not.
   */
  private Optional<String> getRequestId() {
    // See org.jboss.resteasy.reactive.server.injection.ContextProducers
    ResteasyReactiveRequestContext context = CurrentRequestManager.get();
    if (context != null) {
      ContainerRequestContextImpl request = context.getContainerRequestContext();
      String requestId = (String) request.getProperty(RequestIdFilter.REQUEST_ID_KEY);
      return Optional.ofNullable(requestId);
    }
    return Optional.empty();
  }

  /** Extracts the OpenTelemetry context from the current span. */
  private Map<String, String> getOpenTelemetryContext() {
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      return Map.of(
          "otel.span_id",
          spanContext.getSpanId(),
          "otel.trace_id",
          spanContext.getTraceId(),
          "otel.sampled",
          String.valueOf(spanContext.isSampled()),
          "otel.trace_flags",
          spanContext.getTraceFlags().asHex());
    }
    return Map.of();
  }
}
