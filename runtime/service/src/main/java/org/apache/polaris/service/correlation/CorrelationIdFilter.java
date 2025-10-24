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
package org.apache.polaris.service.correlation;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.config.FilterPriorities;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorrelationIdFilter {

  public static final String CORRELATION_ID_KEY = "requestId";

  private static final Logger LOGGER = LoggerFactory.getLogger(CorrelationIdFilter.class);

  @Inject CorrelationIdConfiguration correlationIdConfiguration;
  @Inject CorrelationIdGenerator correlationIdGenerator;

  @ServerRequestFilter(preMatching = true, priority = FilterPriorities.CORRELATION_ID_FILTER)
  public Uni<Response> assignRequestId(ContainerRequestContext rc) {
    var correlationId = rc.getHeaderString(correlationIdConfiguration.headerName());
    return (correlationId != null
            ? Uni.createFrom().item(correlationId)
            : correlationIdGenerator.generateCorrelationId(rc))
        .onItem()
        .invoke(id -> rc.setProperty(CORRELATION_ID_KEY, id))
        .onItemOrFailure()
        .transform((id, error) -> error == null ? null : errorResponse(error));
  }

  @ServerResponseFilter
  public void addResponseHeader(
      ContainerRequestContext request, ContainerResponseContext response) {
    String correlationId = (String) request.getProperty(CORRELATION_ID_KEY);
    if (correlationId != null) { // can be null if request ID generation fails
      response.getHeaders().add(correlationIdConfiguration.headerName(), correlationId);
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
