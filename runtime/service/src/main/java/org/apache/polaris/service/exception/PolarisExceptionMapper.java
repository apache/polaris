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
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.exceptions.PolarisException;
import org.apache.polaris.core.exceptions.PolarisServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * An {@link ExceptionMapper} implementation for {@link PolarisException}s. Delegates HTTP status
 * resolution to {@link PolarisException#httpStatusCode()}.
 */
@Provider
public class PolarisExceptionMapper implements ExceptionMapper<PolarisException> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PolarisExceptionMapper.class);

  @Override
  public Response toResponse(PolarisException exception) {
    int statusCode = exception.httpStatusCode();
    getLogger()
        .atLevel(statusCode >= 500 ? Level.INFO : Level.DEBUG)
        .setCause(exception)
        .log("Full PolarisException");

    ErrorResponse errorResponse =
        ErrorResponse.builder()
            .responseCode(statusCode)
            .withType(exception.getClass().getSimpleName())
            .withMessage(exception.getMessage())
            .build();
    Response.ResponseBuilder builder =
        Response.status(statusCode).entity(errorResponse).type(MediaType.APPLICATION_JSON_TYPE);
    if (exception instanceof PolarisServiceUnavailableException e
        && e.getRetryAfterSeconds() != 0) {
      builder.header(HttpHeaders.RETRY_AFTER, e.getRetryAfterSeconds());
    }
    return builder.build();
  }

  @VisibleForTesting
  Logger getLogger() {
    return LOGGER;
  }
}
