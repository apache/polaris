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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See Dropwizard's {@code io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper} */
@Provider
public final class IcebergJsonProcessingExceptionMapper
    implements ExceptionMapper<JsonProcessingException> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergJsonProcessingExceptionMapper.class);

  @JsonInclude(Include.NON_NULL)
  public record ErrorMessage(
      @JsonProperty("code") int code,
      @JsonProperty("message") @Nullable String message,
      @JsonProperty("details") @Nullable String details) {}

  @Override
  public Response toResponse(JsonProcessingException exception) {
    /*
     * If the error is in the JSON generation or an invalid definition, it's a server error.
     */
    if (exception instanceof JsonGenerationException
        || exception instanceof InvalidDefinitionException) {
      long id = ThreadLocalRandom.current().nextLong();
      LOGGER.error(String.format(Locale.ROOT, "Error handling a request: %016x", id), exception);
      String message =
          String.format(
              Locale.ROOT,
              "There was an error processing your request. It has been logged (ID %016x).",
              id);
      return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode())
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(new ErrorMessage(500, message, null))
          .build();
    }

    /*
     * Otherwise, it's those pesky users.
     */
    LOGGER.info("Unable to process JSON: {}", exception.getMessage());
    LOGGER.debug("Full JsonProcessingException", exception);

    String messagePrefix =
        switch (exception) {
          case JsonParseException e -> "Invalid JSON: ";
          case ValueInstantiationException ve -> "Invalid value: ";
          default -> "";
        };
    final String message = messagePrefix + exception.getOriginalMessage();
    ErrorResponse icebergErrorResponse =
        ErrorResponse.builder()
            .responseCode(Status.BAD_REQUEST.getStatusCode())
            .withType(exception.getClass().getSimpleName())
            .withMessage(message)
            .build();
    return Response.status(Status.BAD_REQUEST)
        .type(MediaType.APPLICATION_JSON_TYPE)
        .entity(icebergErrorResponse)
        .build();
  }
}
