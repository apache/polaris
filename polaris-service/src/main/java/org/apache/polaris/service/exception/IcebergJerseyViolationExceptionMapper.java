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

import io.dropwizard.jersey.validation.JerseyViolationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * Override of the default JerseyViolationExceptionMapper to provide an Iceberg ErrorResponse with
 * the exception details.
 */
@Provider
public class IcebergJerseyViolationExceptionMapper
    implements ExceptionMapper<JerseyViolationException> {
  @Override
  public Response toResponse(JerseyViolationException exception) {
    final String message = "Invalid value: " + exception.getMessage();
    ErrorResponse icebergErrorResponse =
        ErrorResponse.builder()
            .responseCode(Response.Status.BAD_REQUEST.getStatusCode())
            .withType(exception.getClass().getSimpleName())
            .withMessage(message)
            .build();
    return Response.status(Response.Status.BAD_REQUEST)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(icebergErrorResponse)
        .build();
  }
}
