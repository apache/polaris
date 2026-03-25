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
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.context.RequestIdSupplier;

@ApplicationScoped
public class PolarisEventMetadataFactory {

  @Inject Clock clock;
  @Inject CurrentIdentityAssociation currentIdentityAssociation;
  @Inject Instance<RealmContext> realmContext;
  @Inject RequestIdSupplier requestIdSupplier;

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
   * Extracts the request ID from the current {@link RequestIdSupplier}.
   *
   * <p>On normal HTTP request threads the supplier is backed by the request-scoped {@link
   * org.apache.polaris.service.context.catalog.RequestIdHolder}. On async task threads it is
   * populated by {@link org.apache.polaris.service.task.RequestIdPropagator}.
   */
  private Optional<String> getRequestId() {
    try {
      return Optional.ofNullable(requestIdSupplier.getRequestId());
    } catch (ContextNotActiveException e) {
      // No active request scope (e.g. background thread without @ActivateRequestContext).
      return Optional.empty();
    }
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
