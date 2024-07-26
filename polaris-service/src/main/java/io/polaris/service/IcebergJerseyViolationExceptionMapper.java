package io.polaris.service;

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
