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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.ws.rs.core.Response.Status;
import java.net.URI;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.exception.BigLakeFailureCategory;
import org.apache.polaris.service.exception.BigLakeFederationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class BigLakeFederatedRestClient implements RESTClient {
  static final String LOCAL_CATALOG_NAME_PROPERTY = "polaris.internal.biglake.catalog-name";

  private static final Logger LOGGER = LoggerFactory.getLogger(BigLakeFederatedRestClient.class);
  private static final String REQUEST_COUNTER = "polaris.federation.biglake.requests";
  private static final String REQUEST_LATENCY = "polaris.federation.biglake.request.latency";
  private static final String RETRY_COUNT_PROPERTY = "rest.client.max-retries";

  private final RESTClient delegate;
  private final MeterRegistry meterRegistry;
  private final String localCatalogName;
  private final String remoteHost;
  private final int configuredMaxRetries;

  BigLakeFederatedRestClient(
      RESTClient delegate,
      MeterRegistry meterRegistry,
      String localCatalogName,
      Map<String, String> clientProperties) {
    this.delegate = delegate;
    this.meterRegistry = meterRegistry;
    this.localCatalogName =
        Strings.isNullOrEmpty(localCatalogName) ? "unknown" : localCatalogName;
    this.remoteHost =
        sanitizeRemoteHost(clientProperties.get(org.apache.iceberg.CatalogProperties.URI));
    this.configuredMaxRetries = parseInt(clientProperties.get(RETRY_COUNT_PROPERTY), 0);
  }

  @Override
  public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
    BigLakeOperationType operation = BigLakeOperationType.from("HEAD", path);
    long startedAt = System.nanoTime();
    ErrorCapture capture = new ErrorCapture();
    try {
      delegate.head(path, headers, capture.capture(errorHandler));
      recordSuccess(operation, 200, startedAt);
    } catch (RuntimeException e) {
      throw reclassifyFailure(operation, startedAt, capture, e);
    }
  }

  @Override
  public <T extends RESTResponse> T delete(
      String path,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    BigLakeOperationType operation = BigLakeOperationType.from("DELETE", path);
    long startedAt = System.nanoTime();
    ErrorCapture capture = new ErrorCapture();
    try {
      T response = delegate.delete(path, responseType, headers, capture.capture(errorHandler));
      recordSuccess(operation, 200, startedAt);
      return response;
    } catch (RuntimeException e) {
      throw reclassifyFailure(operation, startedAt, capture, e);
    }
  }

  @Override
  public <T extends RESTResponse> T get(
      String path,
      Map<String, String> queryParams,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    BigLakeOperationType operation = BigLakeOperationType.from("GET", path);
    long startedAt = System.nanoTime();
    ErrorCapture capture = new ErrorCapture();
    try {
      T response =
          delegate.get(
              path,
              queryParams,
              responseType,
              headers,
              capture.capture(errorHandler),
              capture::captureHeaders);
      recordSuccess(operation, 200, startedAt);
      return response;
    } catch (RuntimeException e) {
      throw reclassifyFailure(operation, startedAt, capture, e);
    }
  }

  @Override
  public <T extends RESTResponse> T post(
      String path,
      RESTRequest body,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    BigLakeOperationType operation = BigLakeOperationType.from("POST", path);
    long startedAt = System.nanoTime();
    ErrorCapture capture = new ErrorCapture();
    try {
      T response =
          delegate.post(
              path,
              body,
              responseType,
              headers,
              capture.capture(errorHandler),
              capture::captureHeaders);
      recordSuccess(operation, 200, startedAt);
      return response;
    } catch (RuntimeException e) {
      throw reclassifyFailure(operation, startedAt, capture, e);
    }
  }

  @Override
  public <T extends RESTResponse> T postForm(
      String path,
      Map<String, String> formData,
      Class<T> responseType,
      Map<String, String> headers,
      Consumer<ErrorResponse> errorHandler) {
    BigLakeOperationType operation = BigLakeOperationType.from("POST", path);
    long startedAt = System.nanoTime();
    ErrorCapture capture = new ErrorCapture();
    try {
      T response =
          delegate.postForm(path, formData, responseType, headers, capture.capture(errorHandler));
      recordSuccess(operation, 200, startedAt);
      return response;
    } catch (RuntimeException e) {
      throw reclassifyFailure(operation, startedAt, capture, e);
    }
  }

  @Override
  public RESTClient withAuthSession(AuthSession session) {
    return new BigLakeFederatedRestClient(
        delegate.withAuthSession(session),
        meterRegistry,
        localCatalogName,
        Map.of(org.apache.iceberg.CatalogProperties.URI, "https://" + remoteHost));
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static String sanitizeRemoteHost(String uriString) {
    if (Strings.isNullOrEmpty(uriString)) {
      return "unknown";
    }

    try {
      URI uri = URI.create(uriString);
      if (!Strings.isNullOrEmpty(uri.getHost())) {
        return uri.getHost().toLowerCase(Locale.ROOT);
      }
    } catch (IllegalArgumentException e) {
      // Fall through to avoid propagating logging-only parsing failures.
    }

    return "unknown";
  }

  private RuntimeException reclassifyFailure(
      BigLakeOperationType operation, long startedAt, ErrorCapture capture, RuntimeException failure) {
    BigLakeFederationException classified =
        BigLakeFailureClassifier.classify(
                operation, failure, capture.rawError, capture.responseHeaders, inferredRetryCount(failure))
            .orElse(null);

    if (classified != null) {
      recordFailure(
          operation,
          classified.responseCode(),
          classified.category(),
          startedAt,
          classified.googleRequestId(),
          classified.retryAfter(),
          classified.retryCount());
      return classified;
    }

    int responseStatus = capture.rawError != null ? capture.rawError.code() : 500;
    String googleRequestId = firstHeader(capture.responseHeaders, "x-goog-request-id", "x-request-id");
    String retryAfter = firstHeader(capture.responseHeaders, "retry-after");
    int retryCount = inferredRetryCount(failure);
    recordFailure(
        operation,
        responseStatus,
        BigLakeFailureCategory.UNKNOWN,
        startedAt,
        googleRequestId,
        retryAfter,
        retryCount);
    return new BigLakeFederationException(
        BigLakeFailureCategory.UNKNOWN,
        "BigLakeUnknownException",
        responseStatus > 0 ? responseStatus : Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        responseStatus,
        googleRequestId,
        retryAfter,
        retryCount,
        buildUnknownFailureMessage(responseStatus, googleRequestId, retryAfter),
        failure);
  }

  private void recordSuccess(BigLakeOperationType operation, int responseStatus, long startedAt) {
    Duration latency = Duration.ofNanos(System.nanoTime() - startedAt);
    Tags tags =
        Tags.of(
            "operation",
            operation.name(),
            "remote_host",
            remoteHost,
            "response_status",
            Integer.toString(responseStatus),
            "failure_category",
            "NONE",
            "outcome",
            "success");
    meterRegistry.counter(REQUEST_COUNTER, tags).increment();
    Timer.builder(REQUEST_LATENCY).tags(tags).register(meterRegistry).record(latency);

    LOGGER
        .atDebug()
        .addKeyValue("catalog", localCatalogName)
        .addKeyValue("operation", operation.name())
        .addKeyValue("remoteHost", remoteHost)
        .addKeyValue("responseStatus", responseStatus)
        .addKeyValue("latencyMs", latency.toMillis())
        .addKeyValue("retryCount", 0)
        .addKeyValue("failureCategory", "NONE")
        .log("BigLake outbound request completed");
  }

  private void recordFailure(
      BigLakeOperationType operation,
      int responseStatus,
      BigLakeFailureCategory failureCategory,
      long startedAt,
      String googleRequestId,
      String retryAfter,
      int retryCount) {
    Duration latency = Duration.ofNanos(System.nanoTime() - startedAt);
    Tags tags =
        Tags.of(
            "operation",
            operation.name(),
            "remote_host",
            remoteHost,
            "response_status",
            Integer.toString(responseStatus),
            "failure_category",
            failureCategory.name(),
            "outcome",
            "failure");
    meterRegistry.counter(REQUEST_COUNTER, tags).increment();
    Timer.builder(REQUEST_LATENCY).tags(tags).register(meterRegistry).record(latency);

    var log =
        LOGGER
            .atInfo()
            .addKeyValue("catalog", localCatalogName)
            .addKeyValue("operation", operation.name())
            .addKeyValue("remoteHost", remoteHost)
            .addKeyValue("responseStatus", responseStatus)
            .addKeyValue("latencyMs", latency.toMillis())
            .addKeyValue("retryCount", retryCount)
            .addKeyValue("failureCategory", failureCategory.name());
    if (!Strings.isNullOrEmpty(googleRequestId)) {
      log.addKeyValue("googleRequestId", googleRequestId);
    }
    if (!Strings.isNullOrEmpty(retryAfter)) {
      log.addKeyValue("retryAfter", retryAfter);
    }
    log.log("BigLake outbound request failed");
  }

  private int inferredRetryCount(Throwable failure) {
    if (failure.getMessage() == null) {
      return 0;
    }

    String message = failure.getMessage();
    int retriesIndex = message.toLowerCase(Locale.ROOT).lastIndexOf("retries");
    if (retriesIndex > 0) {
      String prefix = message.substring(0, retriesIndex);
      for (int i = prefix.length() - 1; i >= 0; i--) {
        if (!Character.isDigit(prefix.charAt(i))) {
          if (i + 1 < prefix.length()) {
            try {
              return Integer.parseInt(prefix.substring(i + 1).trim());
            } catch (NumberFormatException e) {
              break;
            }
          }
          break;
        }
      }
    }
    return configuredMaxRetries == 0 ? 0 : 0;
  }

  private static String firstHeader(Map<String, String> headers, String... names) {
    for (String name : names) {
      for (Map.Entry<String, String> entry : headers.entrySet()) {
        if (entry.getKey().equalsIgnoreCase(name) && !Strings.isNullOrEmpty(entry.getValue())) {
          return entry.getValue();
        }
      }
    }
    return null;
  }

  private static int parseInt(String value, int defaultValue) {
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static String buildUnknownFailureMessage(
      int responseStatus, String googleRequestId, String retryAfter) {
    StringBuilder builder =
        new StringBuilder(
            "BigLake returned an unexpected response that Polaris could not classify safely."
                + " Review Polaris BigLake outbound request telemetry and the remote service"
                + " health for more detail.");
    if (responseStatus > 0) {
      builder.append(" Remote HTTP status: ").append(responseStatus).append('.');
    }
    if (!Strings.isNullOrEmpty(googleRequestId)) {
      builder.append(" Google request ID: ").append(googleRequestId).append('.');
    }
    if (!Strings.isNullOrEmpty(retryAfter)) {
      builder.append(" Retry-After: ").append(retryAfter).append('.');
    }
    return builder.toString();
  }

  static String sanitizeRemoteDetail(String message) {
    if (Strings.isNullOrEmpty(message)) {
      return message;
    }

    String sanitized = message;
    sanitized = sanitized.replaceAll("(?i)bearer\\s+[A-Za-z0-9._\\-+/=]+", "Bearer <redacted>");
    sanitized =
        sanitized.replaceAll(
            "(?i)(access[_-]?token|refresh[_-]?token|client[_-]?secret|private[_-]?key)"
                + "\"?\\s*[:=]\\s*\"[^\"]+\"",
            "$1:\"<redacted>\"");
    sanitized =
        sanitized.replaceAll(
            "(?s)-----BEGIN PRIVATE KEY-----.*?-----END PRIVATE KEY-----", "<redacted>");
    sanitized =
        sanitized.replaceAll(
            "(?i)(authorization|proxy-authorization)\\s*[:=]\\s*[^,\\s]+",
            "$1:<redacted>");
    sanitized =
        sanitized.replaceAll(
            "(?i)([?&](?:token|access_token|refresh_token|client_secret|private_key)=)[^&\\s]+",
            "$1<redacted>");
    return sanitized;
  }

  private static final class ErrorCapture {
    private final Map<String, String> responseHeaders = new LinkedHashMap<>();
    private ErrorResponse rawError;

    private Consumer<ErrorResponse> capture(Consumer<ErrorResponse> delegate) {
      return error -> {
        rawError = error;
        ErrorResponse sanitized =
            ErrorResponse.builder()
                .responseCode(error.code())
                .withType(error.type())
                .withMessage(sanitizeRemoteDetail(error.message()))
                .build();
        delegate.accept(sanitized);
      };
    }

    private void captureHeaders(Map<String, String> headers) {
      if (headers != null) {
        responseHeaders.putAll(headers);
      }
    }
  }
}
