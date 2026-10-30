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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See Dropwizard's {@code io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper} */
@Provider
public class IcebergJsonProcessingExceptionMapper
    implements ExceptionMapper<JsonProcessingException> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergJsonProcessingExceptionMapper.class);
  private static final Pattern MISSING_REQUIRED_PROPERTY =
      Pattern.compile("^Missing required creator property '([^']+)'");

  @Override
  public Response toResponse(JsonProcessingException exception) {
    /*
     * If the error is in the JSON generation or an invalid definition, it's a server error.
     */
    if (exception instanceof JsonGenerationException
        || exception instanceof InvalidDefinitionException) {
      long id = ThreadLocalRandom.current().nextLong();
      getLoggerForExceptionLogging()
          .error(String.format(Locale.ROOT, "Error handling a request: %016x", id), exception);
      String message =
          String.format(
              Locale.ROOT,
              "There was an error processing your request. It has been logged (ID %016x).",
              id);
      ErrorResponse icebergErrorResponse =
          ErrorResponse.builder()
              .responseCode(Status.INTERNAL_SERVER_ERROR.getStatusCode())
              .withType(exception.getClass().getSimpleName())
              .withMessage(message)
              .build();
      return Response.status(Status.INTERNAL_SERVER_ERROR)
          .type(MediaType.APPLICATION_JSON_TYPE)
          .entity(icebergErrorResponse)
          .build();
    }

    /*
     * Otherwise, it's those pesky users.
     */
    LOGGER.info("Unable to process JSON: {}", exception.getMessage());
    getLoggerForExceptionLogging().debug("Full JsonProcessingException", exception);

    final String message = clientMessage(exception);
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

  /**
   * Builds the message returned to the client. Jackson's own messages for a body that does not
   * match the target model name Java types, model classes and the accepted polymorphic subtypes,
   * which are internal detail; the client only needs the property and what is wrong with it. The
   * full exception stays in the debug log above.
   */
  private static String clientMessage(JsonProcessingException exception) {
    return switch (exception) {
      case JsonParseException e -> "Invalid JSON: " + e.getOriginalMessage();
      case ValueInstantiationException e -> "Invalid value: " + e.getOriginalMessage();
      case InvalidTypeIdException e ->
          "Invalid value: unknown type id '" + e.getTypeId() + "'" + forProperty(e);
      case MismatchedInputException e -> {
        Matcher missing = MISSING_REQUIRED_PROPERTY.matcher(e.getOriginalMessage());
        if (missing.find()) {
          yield "Invalid value: missing required property '" + missing.group(1) + "'";
        }
        String property = forProperty(e);
        yield property.isEmpty()
            ? "Invalid value: request body does not match the expected structure"
            : "Invalid value" + property;
      }
      default -> exception.getOriginalMessage();
    };
  }

  /** The JSON path of the offending property, built from field names and indices only. */
  private static String forProperty(JsonMappingException exception) {
    StringBuilder path = new StringBuilder();
    for (JsonMappingException.Reference reference : exception.getPath()) {
      if (reference.getFieldName() != null) {
        if (!path.isEmpty()) {
          path.append('.');
        }
        path.append(reference.getFieldName());
      } else if (reference.getIndex() >= 0) {
        path.append('[').append(reference.getIndex()).append(']');
      }
    }
    return path.isEmpty() ? "" : " for property '" + path + "'";
  }

  /**
   * This function is only present for the {@code ExceptionMapperTest} and must only be used during
   * once any execution of {@link #toResponse(JsonProcessingException)}.
   */
  @VisibleForTesting
  Logger getLoggerForExceptionLogging() {
    return LOGGER;
  }
}
