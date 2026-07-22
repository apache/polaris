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
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.base.Strings;
import jakarta.ws.rs.core.Response.Status;
import java.io.EOFException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.SSLException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.core.exceptions.FileIOUnknownHostException;
import org.apache.polaris.service.exception.BigLakeFailureCategory;
import org.apache.polaris.service.exception.BigLakeFederationException;
import org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException;
import org.jspecify.annotations.Nullable;

final class BigLakeFailureClassifier {
  private static final Set<String> AUTHENTICATION_HINTS =
      Set.of(
          "unauthenticated",
          "invalid authentication credentials",
          "invalid_grant",
          "failed to obtain access token",
          "credential is malformed");

  private static final Set<String> AUTHORIZATION_HINTS =
      Set.of(
          "permission denied",
          "permission_denied",
          "insufficient permissions",
          "does not have permission",
          "iam permission",
          "forbidden");

  private static final Set<String> QUOTA_CONFIGURATION_HINTS =
      Set.of(
          "quota project",
          "x-goog-user-project",
          "user project",
          "consumer invalid",
          "billing",
          "serviceusage.services.use");

  private static final Set<String> QUOTA_EXHAUSTED_HINTS =
      Set.of(
          "quota exceeded",
          "quota exhausted",
          "rate limit exceeded",
          "too many requests",
          "resource exhausted");

  private static final Set<String> UNSUPPORTED_OPERATION_HINTS =
      Set.of("unsupported", "not implemented", "method not allowed");

  private static final Set<String> TLS_HINTS =
      Set.of("ssl", "tls", "certificate", "handshake");

  private BigLakeFailureClassifier() {}

  static Optional<BigLakeFederationException> classify(
      BigLakeOperationType operation,
      Throwable failure,
      @Nullable ErrorResponse remoteError,
      Map<String, String> responseHeaders,
      int retryCount) {
    if (failure instanceof BigLakeFederationException classified) {
      return Optional.of(classified);
    }

    if (failure instanceof NoSuchTableException
        || failure instanceof NoSuchNamespaceException
        || failure instanceof NoSuchViewException) {
      return Optional.empty();
    }

    Throwable root = rootCause(failure);
    int remoteStatus = remoteError != null ? remoteError.code() : inferRemoteStatus(failure);
    String remoteDetail =
        firstNonBlank(
            remoteError != null ? remoteError.message() : null,
            failure.getMessage(),
            root.getMessage());
    String normalizedDetail = normalize(remoteDetail);
    String googleRequestId = extractHeader(responseHeaders, "x-goog-request-id", "x-request-id");
    String retryAfter = extractHeader(responseHeaders, "retry-after");

    if (remoteStatus == Status.UNAUTHORIZED.getStatusCode()
        || containsAny(normalizedDetail, AUTHENTICATION_HINTS)
        || failure instanceof NotAuthorizedException) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.AUTHENTICATION,
              "BigLakeAuthenticationException",
              Status.UNAUTHORIZED.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "BigLake rejected Polaris's outbound Google authentication. Verify the Polaris"
                  + " server's Application Default Credentials or workload identity settings.",
              failure));
    }

    if (remoteStatus == Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return Optional.of(
          buildException(
              containsAny(normalizedDetail, QUOTA_EXHAUSTED_HINTS)
                  ? BigLakeFailureCategory.QUOTA_EXHAUSTED
                  : BigLakeFailureCategory.THROTTLED,
              "BigLakeThrottledException",
              Status.TOO_MANY_REQUESTS.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "BigLake throttled the outbound request. Retry later and honor any Retry-After"
                  + " guidance returned by the remote service.",
              failure));
    }

    if (containsAny(normalizedDetail, QUOTA_CONFIGURATION_HINTS)
        && (remoteStatus == Status.BAD_REQUEST.getStatusCode()
            || remoteStatus == Status.FORBIDDEN.getStatusCode()
            || remoteStatus == Status.NOT_FOUND.getStatusCode())) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.QUOTA_PROJECT_CONFIGURATION,
              "BigLakeQuotaProjectException",
              Status.BAD_REQUEST.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "BigLake rejected the configured quota project. Verify"
                  + " connectionConfigInfo.properties.header.x-goog-user-project and confirm"
                  + " that the billing project is valid and authorized for the remote API.",
              failure));
    }

    if (remoteStatus == Status.FORBIDDEN.getStatusCode()
        || containsAny(normalizedDetail, AUTHORIZATION_HINTS)) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.AUTHORIZATION,
              "BigLakeAuthorizationException",
              Status.FORBIDDEN.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "BigLake denied the outbound request because the configured Google identity lacks"
                  + " the required IAM permissions for the remote catalog or warehouse.",
              failure));
    }

    if (isUnsupported(remoteStatus, normalizedDetail)) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.UNSUPPORTED_OPERATION,
              "BigLakeUnsupportedOperationException",
              Status.NOT_ACCEPTABLE.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "The target BigLake catalog does not support this Iceberg REST operation.",
              failure));
    }

    if (operation == BigLakeOperationType.CONFIG
        && (remoteStatus == Status.NOT_FOUND.getStatusCode()
            || remoteStatus == Status.BAD_REQUEST.getStatusCode()
            || failure instanceof NotFoundException)) {
      BigLakeFailureCategory category =
          normalizedDetail.contains("warehouse")
              ? BigLakeFailureCategory.INVALID_REMOTE_WAREHOUSE
              : BigLakeFailureCategory.INVALID_REMOTE_CATALOG;
      String message =
          category == BigLakeFailureCategory.INVALID_REMOTE_WAREHOUSE
              ? "Polaris could not access the configured BigLake warehouse. Verify"
                  + " connectionConfigInfo.remoteCatalogName and any referenced gs:// warehouse"
                  + " location."
              : "Polaris could not access the configured BigLake remote catalog. Verify"
                  + " connectionConfigInfo.remoteCatalogName, the BigLake endpoint, and the"
                  + " remote IAM permissions.";
      return Optional.of(
          buildException(
              category,
              "BigLakeCatalogConfigurationException",
              Status.BAD_REQUEST.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              message,
              failure));
    }

    if (failure instanceof TimeoutException
        || root instanceof SocketTimeoutException
        || root instanceof InterruptedIOException) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.TIMEOUT,
              "BigLakeTimeoutException",
              Status.SERVICE_UNAVAILABLE.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "The outbound BigLake request timed out. Retry later and verify the remote service"
                  + " and network path are healthy.",
              failure));
    }

    if (failure instanceof FileIOUnknownHostException || root instanceof UnknownHostException) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.DNS_FAILURE,
              "BigLakeDnsException",
              Status.SERVICE_UNAVAILABLE.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "Polaris could not resolve the BigLake host name for the outbound request. Verify"
                  + " DNS and network egress from the Polaris service.",
              failure));
    }

    if (root instanceof SSLException
        || root instanceof CertificateException
        || containsAny(normalizedDetail, TLS_HINTS)) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.TLS_FAILURE,
              "BigLakeTlsException",
              Status.SERVICE_UNAVAILABLE.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "Polaris could not establish a trusted TLS connection to BigLake. Verify the"
                  + " remote certificate chain and Polaris trust configuration.",
              failure));
    }

    if (root instanceof ConnectException
        || root instanceof SocketException
        || root instanceof EOFException) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.CONNECTION_FAILURE,
              "BigLakeConnectionException",
              Status.SERVICE_UNAVAILABLE.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "Polaris could not connect to the remote BigLake service. Verify network egress,"
                  + " proxies, and the remote endpoint health.",
              failure));
    }

    if (remoteStatus >= 500
        || failure instanceof ServiceUnavailableException
        || failure instanceof RESTException
        || failure instanceof RuntimeIOException) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.SERVICE_UNAVAILABLE,
              "BigLakeServiceUnavailableException",
              Status.SERVICE_UNAVAILABLE.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "The remote BigLake or GCP service is temporarily unavailable. Retry later and"
                  + " investigate the remote service health if the issue persists.",
              failure));
    }

    if (containsAny(normalizedDetail, QUOTA_EXHAUSTED_HINTS)) {
      return Optional.of(
          buildException(
              BigLakeFailureCategory.QUOTA_EXHAUSTED,
              "BigLakeQuotaExceededException",
              Status.TOO_MANY_REQUESTS.getStatusCode(),
              remoteStatus,
              googleRequestId,
              retryAfter,
              retryCount,
              "The configured BigLake or GCP quota has been exhausted. Retry later or increase"
                  + " the quota for the configured billing project.",
              failure));
    }

    return Optional.empty();
  }

  private static BigLakeFederationException buildException(
      BigLakeFailureCategory category,
      String errorType,
      int responseCode,
      int remoteStatus,
      @Nullable String googleRequestId,
      @Nullable String retryAfter,
      int retryCount,
      String message,
      Throwable cause) {
    StringBuilder builder = new StringBuilder(message);
    if (remoteStatus > 0) {
      builder.append(" Remote HTTP status: ").append(remoteStatus).append('.');
    }
    if (!Strings.isNullOrEmpty(googleRequestId)) {
      builder.append(" Google request ID: ").append(googleRequestId).append('.');
    }
    if (!Strings.isNullOrEmpty(retryAfter)) {
      builder.append(" Retry-After: ").append(retryAfter).append('.');
    }
    return new BigLakeFederationException(
        category,
        errorType,
        responseCode,
        remoteStatus,
        googleRequestId,
        retryAfter,
        retryCount,
        builder.toString(),
        cause);
  }

  private static boolean isUnsupported(int remoteStatus, String detail) {
    return remoteStatus == Status.METHOD_NOT_ALLOWED.getStatusCode()
        || remoteStatus == Status.NOT_IMPLEMENTED.getStatusCode()
        || containsAny(detail, UNSUPPORTED_OPERATION_HINTS);
  }

  private static boolean containsAny(String detail, Set<String> hints) {
    return hints.stream().anyMatch(detail::contains);
  }

  private static String normalize(@Nullable String detail) {
    return detail == null ? "" : detail.toLowerCase(Locale.ROOT);
  }

  private static @Nullable String extractHeader(Map<String, String> headers, String... candidates) {
    for (String candidate : candidates) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        if (entry.getKey().equalsIgnoreCase(candidate) && !Strings.isNullOrEmpty(entry.getValue())) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  private static int inferRemoteStatus(Throwable failure) {
    if (failure instanceof NotAuthorizedException) {
      return Status.UNAUTHORIZED.getStatusCode();
    }
    if (failure instanceof NotFoundException) {
      return Status.NOT_FOUND.getStatusCode();
    }
    return -1;
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable current = throwable;
    while (current.getCause() != null && current.getCause() != current) {
      current = current.getCause();
    }
    return current;
  }

  private static @Nullable String firstNonBlank(String... values) {
    for (String value : values) {
      if (!Strings.isNullOrEmpty(value) && !value.trim().isEmpty()) {
        return value;
      }
    }
    return null;
  }
}
