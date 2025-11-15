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
import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
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
import org.apache.polaris.core.exceptions.FileIOUnknownHostException;
import org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Provider
public class IcebergExceptionMapper implements ExceptionMapper<RuntimeException> {
  /** Signifies that we could not extract an HTTP code from a given cloud exception */
  public static final int UNKNOWN_CLOUD_HTTP_CODE = -1;

  public static final Set<Integer> RETRYABLE_AZURE_HTTP_CODES =
      Set.of(
          Response.Status.REQUEST_TIMEOUT.getStatusCode(),
          Response.Status.TOO_MANY_REQUESTS.getStatusCode(),
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          Response.Status.SERVICE_UNAVAILABLE.getStatusCode(),
          Response.Status.GATEWAY_TIMEOUT.getStatusCode(),
          IcebergExceptionMapper.UNKNOWN_CLOUD_HTTP_CODE);

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergExceptionMapper.class);

  // Case-insensitive parts of exception messages that a request to a cloud provider was denied due
  // to lack of permissions
  // We may want to consider a change to Iceberg Core to wrap cloud provider IO exceptions to
  // Iceberg ForbiddenException
  private static final Set<String> ACCESS_DENIED_HINTS =
      Set.of("access denied", "not authorized", "forbidden");

  @Override
  public Response toResponse(RuntimeException runtimeException) {
    LOGGER.info("Handling runtimeException {}", runtimeException.getMessage());

    int responseCode = mapExceptionToResponseCode(runtimeException);
    if (responseCode == Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      getLoggerForExceptionLogging()
          .error("Unhandled exception returning INTERNAL_SERVER_ERROR", runtimeException);
    } else {
      getLoggerForExceptionLogging()
          .atLevel(responseCode > 500 ? Level.INFO : Level.DEBUG)
          .setCause(runtimeException)
          .log("Full RuntimeException");
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

  public static boolean containsAnyAccessDeniedHint(String message) {
    String messageLower = message.toLowerCase(Locale.ENGLISH);
    return ACCESS_DENIED_HINTS.stream().anyMatch(messageLower::contains);
  }

  /**
   * Check if the Throwable is retryable for the storage provider
   *
   * @param t the Throwable
   * @return true if the Throwable is retryable
   */
  public static boolean isStorageProviderRetryableException(Throwable t) {
    if (t == null) {
      return false;
    }

    if (t.getMessage() != null && containsAnyAccessDeniedHint(t.getMessage())) {
      return false;
    }

    return switch (t) {
      // GCS
      case BaseServiceException bse -> bse.isRetryable();

      // S3
      case SdkException se -> se.retryable();

      // Azure exceptions don't have a retryable property so we just check the HTTP code
      case HttpResponseException hre ->
          RETRYABLE_AZURE_HTTP_CODES.contains(
              IcebergExceptionMapper.extractHttpCodeFromCloudException(hre));
      default -> true;
    };
  }

  @VisibleForTesting
  public static Collection<String> getAccessDeniedHints() {
    return ImmutableSet.copyOf(ACCESS_DENIED_HINTS);
  }

  static int mapExceptionToResponseCode(RuntimeException rex) {
    for (Throwable t : Throwables.getCausalChain(rex)) {
      // Cloud exceptions can be wrapped by the Iceberg SDK
      Optional<Integer> code = mapCloudExceptionToResponseCode(t);
      if (code.isPresent()) {
        return code.get();
      }
    }

    return switch (rex) {
      case NoSuchNamespaceException e -> Status.NOT_FOUND.getStatusCode();
      case NoSuchIcebergTableException e -> Status.NOT_FOUND.getStatusCode();
      case NoSuchTableException e -> Status.NOT_FOUND.getStatusCode();
      case NoSuchViewException e -> Status.NOT_FOUND.getStatusCode();
      case NotFoundException e -> Status.NOT_FOUND.getStatusCode();
      case FileIOUnknownHostException e -> Status.NOT_FOUND.getStatusCode();
      case AlreadyExistsException e -> Status.CONFLICT.getStatusCode();
      case CommitFailedException e -> Status.CONFLICT.getStatusCode();
      case UnprocessableEntityException e -> 422;
      case CherrypickAncestorCommitException e -> Status.BAD_REQUEST.getStatusCode();
      case CommitStateUnknownException e -> Status.BAD_REQUEST.getStatusCode();
      case DuplicateWAPCommitException e -> Status.BAD_REQUEST.getStatusCode();
      case ForbiddenException e -> Status.FORBIDDEN.getStatusCode();
      case jakarta.ws.rs.ForbiddenException e -> Status.FORBIDDEN.getStatusCode();
      case NotAuthorizedException e -> Status.UNAUTHORIZED.getStatusCode();
      case NamespaceNotEmptyException e -> Status.CONFLICT.getStatusCode();
      case ValidationException e -> Status.BAD_REQUEST.getStatusCode();
      case ServiceUnavailableException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case RuntimeIOException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case ServiceFailureException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case CleanableFailure e -> Status.BAD_REQUEST.getStatusCode();
      case RESTException e -> Status.SERVICE_UNAVAILABLE.getStatusCode();
      case IllegalArgumentException e -> Status.BAD_REQUEST.getStatusCode();
      case UnsupportedOperationException e -> Status.NOT_ACCEPTABLE.getStatusCode();
      case WebApplicationException e -> e.getResponse().getStatus();
      case TimeoutException e -> Status.REQUEST_TIMEOUT.getStatusCode();
      default -> Status.INTERNAL_SERVER_ERROR.getStatusCode();
    };
  }

  /**
   * We typically call cloud providers over HTTP, so when there's an exception there's typically an
   * associated HTTP code. This extracts the HTTP code if possible.
   *
   * @param t The cloud provider throwable
   * @return UNKNOWN_CLOUD_HTTP_CODE if the throwable is not a cloud exception that we know how to
   *     extract the code from
   */
  public static int extractHttpCodeFromCloudException(Throwable t) {
    return switch (t) {
      case S3Exception s3e -> s3e.statusCode();
      case HttpResponseException hre -> hre.getResponse().getStatusCode();
      case StorageException se -> se.getCode();
      default -> UNKNOWN_CLOUD_HTTP_CODE;
    };
  }

  /**
   * Tries mapping a cloud exception to the HTTP code that Polaris should return
   *
   * @param t the throwable/exception
   * @return the HTTP code Polaris should return, if it was possible to return a suitable mapping.
   *     Optional.empty() otherwise.
   */
  static Optional<Integer> mapCloudExceptionToResponseCode(Throwable t) {
    if (!(t instanceof S3Exception
        || t instanceof AzureException
        || t instanceof StorageException)) {
      return Optional.empty();
    }

    if (containsAnyAccessDeniedHint(t.getMessage())) {
      return Optional.of(Status.FORBIDDEN.getStatusCode());
    }

    int httpCode = extractHttpCodeFromCloudException(t);
    Status httpStatus = Status.fromStatusCode(httpCode);
    Status.Family httpFamily = Status.Family.familyOf(httpCode);

    if (httpStatus == Status.NOT_FOUND) {
      return Optional.of(Status.BAD_REQUEST.getStatusCode());
    }
    if (httpStatus == Status.UNAUTHORIZED) {
      return Optional.of(Status.FORBIDDEN.getStatusCode());
    }
    if (httpStatus == Status.BAD_REQUEST
        || httpStatus == Status.FORBIDDEN
        || httpStatus == Status.REQUEST_TIMEOUT
        || httpStatus == Status.TOO_MANY_REQUESTS
        || httpStatus == Status.GATEWAY_TIMEOUT) {
      return Optional.of(httpCode);
    }
    if (httpFamily == Status.Family.REDIRECTION) {
      // Currently Polaris doesn't know how to follow redirects from cloud providers, thus clients
      // shouldn't expect it to.
      // This is a 4xx error to indicate that the client may be able to resolve this by changing
      // some data, such as their catalog's region.
      return Optional.of(422);
    }
    if (httpFamily == Status.Family.SERVER_ERROR) {
      return Optional.of(Status.BAD_GATEWAY.getStatusCode());
    }

    return Optional.of(Status.INTERNAL_SERVER_ERROR.getStatusCode());
  }

  /**
   * This function is only present for the {@code ExceptionMapperTest} and must only be used once
   * during any execution of {@link #toResponse(RuntimeException)}.
   */
  @VisibleForTesting
  Logger getLoggerForExceptionLogging() {
    return LOGGER;
  }
}
