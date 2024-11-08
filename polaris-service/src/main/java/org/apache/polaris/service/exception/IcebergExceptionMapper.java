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

import com.azure.core.exception.AzureException;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CherrypickAncestorCommitException;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.UnprocessableEntityException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class IcebergExceptionMapper implements ExceptionMapper<RuntimeException> {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergExceptionMapper.class);

  // Case-insensitive parts of exception messages that a request to a cloud provider was denied due
  // to lack of permissions
  // We may want to consider a change to Iceberg Core to wrap cloud provider IO exceptions to
  // Iceberg ForbiddenException
  private static final Set<String> ACCESS_DENIED_HINTS =
      Set.of("access denied", "not authorized", "forbidden");

  public IcebergExceptionMapper() {}

  @Override
  public Response toResponse(RuntimeException runtimeException) {
    LOGGER.info("Handling runtimeException {}", runtimeException.getMessage());
    int responseCode =
        switch (runtimeException) {
          case NoSuchNamespaceException e -> Response.Status.NOT_FOUND.getStatusCode();
          case NoSuchIcebergTableException e -> Response.Status.NOT_FOUND.getStatusCode();
          case NoSuchTableException e -> Response.Status.NOT_FOUND.getStatusCode();
          case NoSuchViewException e -> Response.Status.NOT_FOUND.getStatusCode();
          case NotFoundException e -> Response.Status.NOT_FOUND.getStatusCode();
          case AlreadyExistsException e -> Response.Status.CONFLICT.getStatusCode();
          case CommitFailedException e -> Response.Status.CONFLICT.getStatusCode();
          case UnprocessableEntityException e -> 422;
          case CherrypickAncestorCommitException e -> Response.Status.BAD_REQUEST.getStatusCode();
          case CommitStateUnknownException e -> Response.Status.BAD_REQUEST.getStatusCode();
          case DuplicateWAPCommitException e -> Response.Status.BAD_REQUEST.getStatusCode();
          case ForbiddenException e -> Response.Status.FORBIDDEN.getStatusCode();
          case jakarta.ws.rs.ForbiddenException e -> Response.Status.FORBIDDEN.getStatusCode();
          case NotAuthorizedException e -> Response.Status.UNAUTHORIZED.getStatusCode();
          case NamespaceNotEmptyException e -> Response.Status.BAD_REQUEST.getStatusCode();
          case ValidationException e -> Response.Status.BAD_REQUEST.getStatusCode();
          case ServiceUnavailableException e -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
          case RuntimeIOException e -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
          case ServiceFailureException e -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
          case CleanableFailure e -> Response.Status.BAD_REQUEST.getStatusCode();
          case RESTException e -> Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
          case IllegalArgumentException e -> Response.Status.BAD_REQUEST.getStatusCode();
          case UnsupportedOperationException e -> Response.Status.NOT_ACCEPTABLE.getStatusCode();
          case S3Exception e when doesAnyThrowableContainAccessDeniedHint(e) ->
              Response.Status.FORBIDDEN.getStatusCode();
          case AzureException e when doesAnyThrowableContainAccessDeniedHint(e) ->
              Response.Status.FORBIDDEN.getStatusCode();
          case StorageException e when doesAnyThrowableContainAccessDeniedHint(e) ->
              Response.Status.FORBIDDEN.getStatusCode();
          case WebApplicationException e -> e.getResponse().getStatus();
          default -> Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
        };
    if (responseCode == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      LOGGER.error("Unhandled exception returning INTERNAL_SERVER_ERROR", runtimeException);
    }

    ErrorResponse icebergErrorResponse =
        ErrorResponse.builder()
            .responseCode(responseCode)
            .withType(runtimeException.getClass().getSimpleName())
            .withMessage(runtimeException.getMessage())
            .build();
    Response errorResp =
        Response.status(responseCode)
            .entity(icebergErrorResponse)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .build();
    LOGGER.debug("Mapped exception to errorResp: {}", errorResp);
    return errorResp;
  }

  /**
   * @return whether any throwable in the exception chain case-insensitive-contains the given
   *     message
   */
  static boolean doesAnyThrowableContainAccessDeniedHint(Exception e) {
    return Arrays.stream(ExceptionUtils.getThrowables(e))
        .anyMatch(t -> containsAnyAccessDeniedHint(t.getMessage()));
  }

  public static boolean containsAnyAccessDeniedHint(String message) {
    String messageLower = message.toLowerCase(Locale.ENGLISH);
    return ACCESS_DENIED_HINTS.stream().anyMatch(messageLower::contains);
  }

  @VisibleForTesting
  public static Collection<String> getAccessDeniedHints() {
    return ImmutableSet.copyOf(ACCESS_DENIED_HINTS);
  }
}
