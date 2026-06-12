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
package org.apache.polaris.service.exception;

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.exceptions.CommitConflictException;
import org.apache.polaris.core.exceptions.PolarisException;
import org.apache.polaris.core.persistence.PolicyMappingAlreadyExistsException;
import org.apache.polaris.core.policy.exceptions.NoSuchPolicyException;
import org.apache.polaris.core.policy.exceptions.PolicyAttachException;
import org.apache.polaris.core.policy.exceptions.PolicyInUseException;
import org.apache.polaris.core.policy.exceptions.PolicyVersionMismatchException;
import org.apache.polaris.core.policy.validator.InvalidPolicyException;
import org.apache.polaris.service.idempotency.IdempotencyConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * An {@link ExceptionMapper} implementation for {@link PolarisException}s modeled after {@link
 * IcebergExceptionMapper}
 */
@Provider
public class PolarisExceptionMapper implements ExceptionMapper<PolarisException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisExceptionMapper.class);

  // 422 has no constant in jakarta.ws.rs Response.Status, so status codes are modeled as ints.
  private static final int UNPROCESSABLE_ENTITY = 422;

  private int getStatusCode(PolarisException exception) {
    return switch (exception) {
      case AlreadyExistsException alreadyExistsException ->
          Response.Status.CONFLICT.getStatusCode();
      case CommitConflictException commitConflictException ->
          Response.Status.CONFLICT.getStatusCode();
      case InvalidPolicyException invalidPolicyException ->
          Response.Status.BAD_REQUEST.getStatusCode();
      case PolicyAttachException policyAttachException ->
          Response.Status.BAD_REQUEST.getStatusCode();
      case NoSuchPolicyException noSuchPolicyException -> Response.Status.NOT_FOUND.getStatusCode();
      case PolicyVersionMismatchException policyVersionMismatchException ->
          Response.Status.CONFLICT.getStatusCode();
      case PolicyMappingAlreadyExistsException policyMappingAlreadyExistsException ->
          Response.Status.CONFLICT.getStatusCode();
      case PolicyInUseException policyInUseException -> Response.Status.BAD_REQUEST.getStatusCode();
      case IdempotencyConflictException idempotencyConflictException -> UNPROCESSABLE_ENTITY;
      default -> Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
    };
  }

  @Override
  public Response toResponse(PolarisException exception) {
    int statusCode = getStatusCode(exception);
    getLogger()
        .atLevel(
            Response.Status.Family.familyOf(statusCode) == Response.Status.Family.SERVER_ERROR
                ? Level.INFO
                : Level.DEBUG)
        .setCause(exception)
        .log("Full PolarisException");

    ErrorResponse errorResponse =
        ErrorResponse.builder()
            .responseCode(statusCode)
            .withType(exception.getClass().getSimpleName())
            .withMessage(exception.getMessage())
            .build();
    return Response.status(statusCode)
        .entity(errorResponse)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }

  @VisibleForTesting
  Logger getLogger() {
    return LOGGER;
  }
}
