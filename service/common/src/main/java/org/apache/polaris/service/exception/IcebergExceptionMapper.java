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
import com.azure.core.exception.HttpResponseException;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Provider
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
    int responseCode = mapExceptionToResponseCode(runtimeException);
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

  static int mapExceptionToResponseCode(RuntimeException rex) {
    // Cloud exceptions
    if (rex instanceof S3Exception
        || rex instanceof AzureException
        || rex instanceof StorageException) {
      return mapCloudExceptionToResponseCode(rex);
    }

    // Non-cloud exceptions
    return switch (rex) {
      case NoSuchNamespaceException e -> Status.NOT_FOUND.getStatusCode();
      case NoSuchIcebergTableException e -> Status.NOT_FOUND.getStatusCode();
      case NoSuchTableException e -> Status.NOT_FOUND.getStatusCode();
      case NoSuchViewException e -> Status.NOT_FOUND.getStatusCode();
      case NotFoundException e -> Status.NOT_FOUND.getStatusCode();
      case AlreadyExistsException e -> Status.CONFLICT.getStatusCode();
      case CommitFailedException e -> Status.CONFLICT.getStatusCode();
      case UnprocessableEntityException e -> 422;
      case CherrypickAncestorCommitException e -> Status.BAD_REQUEST.getStatusCode();
      case CommitStateUnknownException e -> Status.BAD_REQUEST.getStatusCode();
      case DuplicateWAPCommitException e -> Status.BAD_REQUEST.getStatusCode();
      case ForbiddenException e -> Status.FORBIDDEN.getStatusCode();
      case jakarta.ws.rs.ForbiddenException e -> Status.FORBIDDEN.getStatusCode();
      case NotAuthorizedException e -> Status.UNAUTHORIZED.getStatusCode();
      case NamespaceNotEmptyException e -> Status.BAD_REQUEST.getStatusCode();
      case ValidationException e -> Status.BAD_REQUEST.getStatusCode();
      case ServiceUnavailableException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case RuntimeIOException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case ServiceFailureException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case CleanableFailure e -> Status.BAD_REQUEST.getStatusCode();
      case RESTException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case IllegalArgumentException e -> Status.BAD_REQUEST.getStatusCode();
      case UnsupportedOperationException e -> Status.NOT_ACCEPTABLE.getStatusCode();
      case WebApplicationException e -> e.getResponse().getStatus();
      default -> Status.INTERNAL_SERVER_ERROR.getStatusCode();
    };
  }

  static int mapCloudExceptionToResponseCode(RuntimeException rex) {
    if (doesAnyThrowableContainAccessDeniedHint(rex)) {
      return Status.FORBIDDEN.getStatusCode();
    }

    int httpCode =
        switch (rex) {
          case S3Exception s3e -> s3e.statusCode();
          case HttpResponseException hre -> hre.getResponse().getStatusCode();
          case StorageException se -> se.getCode();
          default -> -1;
        };
    Status httpStatus = Status.fromStatusCode(httpCode);
    Status.Family httpFamily = Status.Family.familyOf(httpCode);

    if (httpStatus == Status.NOT_FOUND) {
      return Status.BAD_REQUEST.getStatusCode();
    }
    if (httpStatus == Status.UNAUTHORIZED) {
      return Status.FORBIDDEN.getStatusCode();
    }
    if (httpStatus == Status.BAD_REQUEST
        || httpStatus == Status.FORBIDDEN
        || httpStatus == Status.REQUEST_TIMEOUT
        || httpStatus == Status.TOO_MANY_REQUESTS
        || httpStatus == Status.GATEWAY_TIMEOUT) {
      return httpCode;
    }
    if (httpFamily == Status.Family.REDIRECTION) {
      // Currently Polaris doesn't know how to follow redirects from cloud providers, thus clients
      // shouldn't expect it to.
      // This is a 4xx error to indicate that the client may be able to resolve this by changing
      // some data, such as their catalog's region.
      return 422;
    }
    if (httpFamily == Status.Family.SERVER_ERROR) {
      return Status.BAD_GATEWAY.getStatusCode();
    }

    return Status.INTERNAL_SERVER_ERROR.getStatusCode();
  }
}
