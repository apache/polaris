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

import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import org.apache.polaris.service.config.FilterPriorities;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;

/**
 * Filter that handles request IDs for tracing.
 *
 * <p>See <a
 * href="https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/observability/tracing">Envoy
 * Tracing</a>
 *
 * @see <a href="https://devcenter.heroku.com/articles/http-request-id">Heroku - HTTP Request
 *     IDs</a>
 */
public class RequestIdFilter {

  public static final String REQUEST_ID_KEY = "requestId";

  @Inject TracingConfiguration tracingConfiguration;

  @ServerRequestFilter(preMatching = true, priority = FilterPriorities.REQUEST_ID_FILTER)
  public void extractRequestId(ContainerRequestContext rc) {
    String requestId = rc.getHeaderString(tracingConfiguration.requestId().headerName());
    if (requestId != null) {
      rc.setProperty(REQUEST_ID_KEY, requestId);
    }
  }

  @ServerResponseFilter
  public void addResponseHeader(
      ContainerRequestContext request, ContainerResponseContext response) {
    String requestId = (String) request.getProperty(REQUEST_ID_KEY);
    if (requestId != null) {
      response.getHeaders().add(tracingConfiguration.requestId().headerName(), requestId);
    }
  }
}
