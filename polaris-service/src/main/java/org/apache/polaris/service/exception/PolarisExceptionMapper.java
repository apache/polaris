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

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.exceptions.AlreadyExistsException;
import org.apache.polaris.core.exceptions.PolarisException;

/**
 * An {@link ExceptionMapper} implementation for {@link PolarisException}s modeled after {@link
 * IcebergExceptionMapper}
 */
@Provider
public class PolarisExceptionMapper implements ExceptionMapper<PolarisException> {

  private Response.Status getStatus(PolarisException exception) {
    if (exception instanceof AlreadyExistsException) {
      return Response.Status.CONFLICT;
    } else {
      return Response.Status.INTERNAL_SERVER_ERROR;
    }
  }

  @Override
  public Response toResponse(PolarisException exception) {
    Response.Status status = getStatus(exception);
    ErrorResponse errorResponse =
        ErrorResponse.builder()
            .responseCode(status.getStatusCode())
            .withType(exception.getClass().getSimpleName())
            .withMessage(exception.getMessage())
            .build();
    return Response.status(status)
        .entity(errorResponse)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .build();
  }
}
