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
package org.apache.polaris.service.context;

import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.config.FilterPriorities;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RealmContextFilter {

  public static final String REALM_CONTEXT_KEY = "realmContext";

  private static final Logger LOGGER = LoggerFactory.getLogger(RealmContextFilter.class);

  @Inject RealmContextResolver realmContextResolver;

  @ServerRequestFilter(preMatching = true, priority = FilterPriorities.REALM_CONTEXT_FILTER)
  public Uni<Response> resolveRealmContext(ContainerRequestContext rc) {
    return Uni.createFrom()
        .completionStage(
            () ->
                realmContextResolver.resolveRealmContext(
                    rc.getUriInfo().getRequestUri().toString(),
                    rc.getMethod(),
                    rc.getUriInfo().getPath(),
                    rc.getHeaders()::getFirst))
        .onItem()
        .invoke(realmContext -> rc.setProperty(REALM_CONTEXT_KEY, realmContext))
        // ContextLocals is used by RealmIdTagContributor to add the realm id to metrics
        .invoke(realmContext -> ContextLocals.put(REALM_CONTEXT_KEY, realmContext))
        .onItemOrFailure()
        .transform((realmContext, error) -> error == null ? null : errorResponse(error));
  }

  private static Response errorResponse(Throwable error) {
    LOGGER.error("Error resolving realm context", error);
    return Response.status(Response.Status.NOT_FOUND)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            ErrorResponse.builder()
                .responseCode(Response.Status.NOT_FOUND.getStatusCode())
                .withMessage("Missing or invalid realm")
                .withType("MissingOrInvalidRealm")
                .build())
        .build();
  }
}
