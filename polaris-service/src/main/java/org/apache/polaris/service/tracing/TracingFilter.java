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
package org.apache.polaris.service.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.semconv.HttpAttributes;
import io.opentelemetry.semconv.ServerAttributes;
import io.opentelemetry.semconv.UrlAttributes;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Priorities;
import java.io.IOException;
import org.apache.polaris.core.context.CallContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Servlet {@link Filter} that starts an OpenTracing {@link Span}, propagating the calling context
 * from HTTP headers, if present. "spanId" and "traceId" are added to the logging MDC so that all
 * logs recorded in the request will contain the current span and trace id. Downstream HTTP calls
 * should use the OpenTelemetry {@link io.opentelemetry.context.propagation.ContextPropagators} to
 * include the current trace id in the request headers.
 */
@Priority(Priorities.AUTHENTICATION - 1)
public class TracingFilter implements Filter {
  private static final Logger LOGGER = LoggerFactory.getLogger(TracingFilter.class);
  private final OpenTelemetry openTelemetry;

  @Inject
  public TracingFilter(OpenTelemetry openTelemetry) {
    this.openTelemetry = openTelemetry;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    Context extractedContext =
        openTelemetry
            .getPropagators()
            .getTextMapPropagator()
            .extract(Context.current(), httpRequest, new HeadersMapAccessor());
    try (Scope scope = extractedContext.makeCurrent()) {
      Tracer tracer = openTelemetry.getTracer(httpRequest.getPathInfo());
      Span span =
          tracer
              .spanBuilder(httpRequest.getMethod() + " " + httpRequest.getPathInfo())
              .setSpanKind(SpanKind.SERVER)
              .setAttribute(
                  "realm", CallContext.getCurrentContext().getRealmContext().getRealmIdentifier())
              .startSpan();

      try (Scope ignored = span.makeCurrent();
          MDC.MDCCloseable spanId = MDC.putCloseable("spanId", span.getSpanContext().getSpanId());
          MDC.MDCCloseable traceId =
              MDC.putCloseable("traceId", span.getSpanContext().getTraceId())) {
        LOGGER
            .atInfo()
            .addKeyValue("spanId", span.getSpanContext().getSpanId())
            .addKeyValue("traceId", span.getSpanContext().getTraceId())
            .addKeyValue("parentContext", extractedContext)
            .log("Started span with parent");
        span.setAttribute(HttpAttributes.HTTP_REQUEST_METHOD, httpRequest.getMethod());
        span.setAttribute(ServerAttributes.SERVER_ADDRESS, httpRequest.getServerName());
        span.setAttribute(UrlAttributes.URL_SCHEME, httpRequest.getScheme());
        span.setAttribute(UrlAttributes.URL_PATH, httpRequest.getPathInfo());

        chain.doFilter(request, response);
      } finally {
        span.end();
      }
    }
  }
}
