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
package org.apache.polaris.service.idempotency;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.util.UUID;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.config.FilterPriorities;

/**
 * Captures the {@code Idempotency-Key} request header into the request-scoped {@link
 * IdempotencyRequestContext} when idempotency is enabled, so downstream handlers and the catalog
 * can apply it without each endpoint parsing the header itself. This mirrors the way {@code
 * RealmContextFilter} populates the request-scoped realm context.
 */
@Provider
@ApplicationScoped
@Priority(FilterPriorities.IDEMPOTENCY_KEY_FILTER)
public class IdempotencyKeyFilter implements ContainerRequestFilter {

  public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

  private final IdempotencyConfiguration idempotencyConfiguration;
  private final IdempotencyRequestContext idempotencyRequestContext;

  @Inject
  public IdempotencyKeyFilter(
      IdempotencyConfiguration idempotencyConfiguration,
      IdempotencyRequestContext idempotencyRequestContext) {
    this.idempotencyConfiguration = idempotencyConfiguration;
    this.idempotencyRequestContext = idempotencyRequestContext;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    if (!idempotencyConfiguration.enabled()) {
      return;
    }
    String header = requestContext.getHeaderString(IDEMPOTENCY_KEY_HEADER);
    if (header == null || header.isBlank()) {
      return;
    }
    UUID key;
    try {
      key = UUID.fromString(header.trim());
    } catch (IllegalArgumentException e) {
      requestContext.abortWith(invalidKeyResponse());
      return;
    }
    idempotencyRequestContext.setPendingKey(key);
  }

  private static Response invalidKeyResponse() {
    return Response.status(Response.Status.BAD_REQUEST)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(
            ErrorResponse.builder()
                .responseCode(Response.Status.BAD_REQUEST.getStatusCode())
                .withMessage("Invalid Idempotency-Key header; expected a UUID")
                .withType("InvalidIdempotencyKey")
                .build())
        .build();
  }
}
