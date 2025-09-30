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

import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.config.FilterPriorities;
import org.apache.polaris.service.logging.LoggingConfiguration;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestIdFilter {

  public static final String REQUEST_ID_KEY = "requestId";

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestIdFilter.class);

  @Inject LoggingConfiguration loggingConfiguration;
  @Inject RequestIdGenerator requestIdGenerator;

  @ServerRequestFilter(preMatching = true, priority = FilterPriorities.REQUEST_ID_FILTER)
  public Uni<Response> assignRequestId(ContainerRequestContext rc) {
    var requestId = rc.getHeaderString(loggingConfiguration.requestIdHeaderName());
    return (requestId != null
            ? Uni.createFrom().item(requestId)
            : requestIdGenerator.generateRequestId(rc))
        .onItem()
        .invoke(id -> rc.setProperty(REQUEST_ID_KEY, id))
        .invoke(id -> ContextLocals.put(REQUEST_ID_KEY, id))
        .onItemOrFailure()
        .transform((id, error) -> error == null ? null : errorResponse(error));
  }

  @ServerResponseFilter
  public void addResponseHeader(
      ContainerRequestContext request, ContainerResponseContext response) {
    String requestId = (String) request.getProperty(REQUEST_ID_KEY);
    if (requestId != null) { // can be null if request ID generation fails
      response.getHeaders().add(loggingConfiguration.requestIdHeaderName(), requestId);
    }
  }

  private static Response errorResponse(Throwable error) {
    LOGGER.error("Failed to generate request ID", error);
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            ErrorResponse.builder()
                .responseCode(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
                .withMessage("Request ID generation failed")
                .withType("RequestIdGenerationError")
                .build())
        .build();
  }
}
