package io.polaris.service;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import io.dropwizard.jersey.errors.LoggingExceptionMapper;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * Override of the default JsonProcessingExceptionMapper to provide an Iceberg ErrorResponse with
 * the exception details. This code mostly comes from Dropwizard's {@link
 * io.dropwizard.jersey.jackson.JsonProcessingExceptionMapper}
 */
@Provider
public final class IcebergJsonProcessingExceptionMapper
    extends LoggingExceptionMapper<JsonProcessingException> {
  @Override
  public Response toResponse(JsonProcessingException exception) {
    /*
     * If the error is in the JSON generation or an invalid definition, it's a server error.
     */
    if (exception instanceof JsonGenerationException
        || exception instanceof InvalidDefinitionException) {
      return super.toResponse(exception); // LoggingExceptionMapper will log exception
    }

    /*
     * Otherwise, it's those pesky users.
     */
    logger.info("Unable to process JSON: {}", exception.getMessage());

    String messagePrefix =
        switch (exception) {
          case JsonParseException e -> "Invalid JSON: ";
          case ValueInstantiationException ve -> "Invalid value: ";
          default -> "";
        };
    final String message = messagePrefix + exception.getOriginalMessage();
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
