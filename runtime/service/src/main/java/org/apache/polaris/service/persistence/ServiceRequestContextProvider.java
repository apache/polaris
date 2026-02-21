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
package org.apache.polaris.service.persistence;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.security.Principal;
import org.apache.polaris.core.persistence.metrics.RequestContextProvider;
import org.apache.polaris.service.tracing.RequestIdFilter;
import org.jboss.resteasy.reactive.server.core.CurrentRequestManager;
import org.jboss.resteasy.reactive.server.core.ResteasyReactiveRequestContext;
import org.jboss.resteasy.reactive.server.jaxrs.ContainerRequestContextImpl;

/**
 * Request-scoped implementation of {@link RequestContextProvider} that obtains context fields from
 * CDI beans and request-scoped objects.
 */
@RequestScoped
public class ServiceRequestContextProvider implements RequestContextProvider {

  private final SecurityContext securityContext;

  @Inject
  public ServiceRequestContextProvider(SecurityContext securityContext) {
    this.securityContext = securityContext;
  }

  @Override
  public String getPrincipalName() {
    if (securityContext != null) {
      Principal principal = securityContext.getUserPrincipal();
      if (principal != null) {
        return principal.getName();
      }
    }
    return null;
  }

  @Override
  public String getRequestId() {
    ResteasyReactiveRequestContext context = CurrentRequestManager.get();
    if (context != null) {
      ContainerRequestContextImpl request = context.getContainerRequestContext();
      if (request != null) {
        return (String) request.getProperty(RequestIdFilter.REQUEST_ID_KEY);
      }
    }
    return null;
  }

  @Override
  public String getOtelTraceId() {
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      return spanContext.getTraceId();
    }
    return null;
  }

  @Override
  public String getOtelSpanId() {
    SpanContext spanContext = Span.current().getSpanContext();
    if (spanContext.isValid()) {
      return spanContext.getSpanId();
    }
    return null;
  }
}
