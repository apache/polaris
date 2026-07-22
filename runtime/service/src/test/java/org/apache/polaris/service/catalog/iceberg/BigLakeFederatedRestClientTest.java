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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.polaris.service.distcache.HttpTestServer;
import org.apache.polaris.service.exception.BigLakeFailureCategory;
import org.apache.polaris.service.exception.BigLakeFederationException;
import org.junit.jupiter.api.Test;

class BigLakeFederatedRestClientTest {
  @Test
  void recordsSuccessMetrics() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FakeRestClient delegate = new FakeRestClient();
    delegate.response = new TestResponse();

    BigLakeFederatedRestClient client =
        new BigLakeFederatedRestClient(
            delegate,
            registry,
            "local-catalog",
            Map.of(
                org.apache.iceberg.CatalogProperties.URI,
                "https://biglake.googleapis.com/iceberg/v1/restcatalog"));

    TestResponse response =
        client.get(
            "/v1/config",
            Map.of(),
            TestResponse.class,
            Map.of(),
            error -> {
              throw new RuntimeException(error.message());
            });

    assertThat(response).isNotNull();
    assertThat(
            registry
                .find("polaris.federation.biglake.requests")
                .tags(
                    "operation",
                    "CONFIG",
                    "remote_host",
                    "biglake.googleapis.com",
                    "response_status",
                    "200",
                    "failure_category",
                    "NONE",
                    "outcome",
                    "success")
                .counter())
        .isNotNull();
    assertThat(registry.find("polaris.federation.biglake.request.latency").timers()).hasSize(1);
  }

  @Test
  void redactsSecretsAndRecordsFailureMetrics() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FakeRestClient delegate = new FakeRestClient();
    delegate.errorResponse =
        ErrorResponse.builder()
            .responseCode(400)
            .withType("BadRequest")
            .withMessage("Missing quota project. Authorization: Bearer super-secret-token")
            .build();
    delegate.responseHeaders = Map.of("x-goog-request-id", "req-1", "Retry-After", "120");

    BigLakeFederatedRestClient client =
        new BigLakeFederatedRestClient(
            delegate,
            registry,
            "local-catalog",
            Map.of(
                org.apache.iceberg.CatalogProperties.URI,
                "https://biglake.googleapis.com/iceberg/v1/restcatalog"));

    assertThatThrownBy(
            () ->
                client.get(
                    "/v1/config",
                    Map.of(),
                    TestResponse.class,
                    Map.of(),
                    error -> {
                      throw new RuntimeException(error.message());
                    }))
        .isInstanceOf(BigLakeFederationException.class)
        .satisfies(
            throwable -> {
              BigLakeFederationException exception = (BigLakeFederationException) throwable;
              assertThat(exception.category())
                  .isEqualTo(BigLakeFailureCategory.QUOTA_PROJECT_CONFIGURATION);
              assertThat(exception.message()).doesNotContain("super-secret-token");
              assertThat(exception.retryAfter()).isEqualTo("120");
            });

    assertThat(
            registry
                .find("polaris.federation.biglake.requests")
                .tags(
                    "operation",
                    "CONFIG",
                    "remote_host",
                    "biglake.googleapis.com",
                    "response_status",
                    "400",
                    "failure_category",
                    "QUOTA_PROJECT_CONFIGURATION",
                    "outcome",
                    "failure")
                .counter())
        .isNotNull();
  }

  @Test
  void classifiesQuotaExhaustionAsRetryable() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FakeRestClient delegate = new FakeRestClient();
    delegate.errorResponse =
        ErrorResponse.builder()
            .responseCode(429)
            .withType("TooManyRequests")
            .withMessage("quota exceeded for billing project")
            .build();
    delegate.responseHeaders = Map.of("Retry-After", "30");

    BigLakeFederatedRestClient client =
        new BigLakeFederatedRestClient(
            delegate,
            registry,
            "local-catalog",
            Map.of(
                org.apache.iceberg.CatalogProperties.URI,
                "https://biglake.googleapis.com/iceberg/v1/restcatalog"));

    assertThatThrownBy(
            () ->
                client.get(
                    "/v1/namespaces/ns/tables/table",
                    Map.of(),
                    TestResponse.class,
                    Map.of(),
                    error -> {
                      throw new RuntimeException(error.message());
                    }))
        .isInstanceOf(BigLakeFederationException.class)
        .satisfies(
            throwable -> {
              BigLakeFederationException exception = (BigLakeFederationException) throwable;
              assertThat(exception.category()).isEqualTo(BigLakeFailureCategory.QUOTA_EXHAUSTED);
              assertThat(exception.category().retryable()).isTrue();
              assertThat(exception.retryAfter()).isEqualTo("30");
            });
  }

  @Test
  void wrapsUnknownFailuresInSanitizedBigLakeException() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    FakeRestClient delegate = new FakeRestClient();
    delegate.runtimeFailure =
        new RuntimeException("Malformed payload Authorization: Bearer super-secret-token");
    delegate.responseHeaders = Map.of("x-goog-request-id", "req-9");

    BigLakeFederatedRestClient client =
        new BigLakeFederatedRestClient(
            delegate,
            registry,
            "local-catalog",
            Map.of(
                org.apache.iceberg.CatalogProperties.URI,
                "https://biglake.googleapis.com/iceberg/v1/restcatalog"));

    assertThatThrownBy(
            () ->
                client.get(
                    "/v1/namespaces/ns/tables/table",
                    Map.of(),
                    TestResponse.class,
                    Map.of(),
                    error -> {
                      throw new RuntimeException(error.message());
                    }))
        .isInstanceOf(BigLakeFederationException.class)
        .satisfies(
            throwable -> {
              BigLakeFederationException exception = (BigLakeFederationException) throwable;
              assertThat(exception.category()).isEqualTo(BigLakeFailureCategory.UNKNOWN);
              assertThat(exception.errorType()).isEqualTo("BigLakeUnknownException");
              assertThat(exception.getMessage())
                  .contains("unexpected response")
                  .contains("req-9")
                  .doesNotContain("super-secret-token");
            });

    assertThat(
            registry
                .find("polaris.federation.biglake.requests")
                .tags(
                    "operation",
                    "LOAD_TABLE",
                    "remote_host",
                    "biglake.googleapis.com",
                    "response_status",
                    "500",
                    "failure_category",
                    "UNKNOWN",
                    "outcome",
                    "failure")
                .counter())
        .isNotNull();
  }

  @Test
  void retriesSupportedTransientFailuresWithConfiguredPolicy() throws IOException {
    AtomicInteger attempts = new AtomicInteger();

    try (HttpTestServer server =
            new HttpTestServer(
                "/v1/config",
                exchange -> {
                  int attempt = attempts.incrementAndGet();
                  sendErrorResponse(
                      exchange,
                      503,
                      "ServiceUnavailable",
                      "temporarily unavailable",
                      "req-retry-" + attempt,
                      "1");
                });
        BigLakeFederatedRestClient client = httpBackedClient(server, 2)) {
      assertThatThrownBy(() -> invokeConfigRequest(client))
          .isInstanceOf(BigLakeFederationException.class)
          .satisfies(
              throwable -> {
                BigLakeFederationException exception = (BigLakeFederationException) throwable;
                assertThat(exception.category())
                    .isEqualTo(BigLakeFailureCategory.SERVICE_UNAVAILABLE);
                assertThat(exception.category().retryable()).isTrue();
                assertThat(exception.retryCount()).isEqualTo(2);
                assertThat(exception.retryAfter()).isEqualTo("1");
              });
    }

    assertThat(attempts).hasValue(3);
  }

  @Test
  void doesNotRetryNonRetryableFailures() throws IOException {
    AtomicInteger attempts = new AtomicInteger();

    try (HttpTestServer server =
            new HttpTestServer(
                "/v1/config",
                exchange -> {
                  attempts.incrementAndGet();
                  sendErrorResponse(
                      exchange,
                      400,
                      "BadRequest",
                      "Missing quota project x-goog-user-project",
                      "req-no-retry",
                      null);
                });
        BigLakeFederatedRestClient client = httpBackedClient(server, 2)) {
      assertThatThrownBy(() -> invokeConfigRequest(client))
          .isInstanceOf(BigLakeFederationException.class)
          .satisfies(
              throwable -> {
                BigLakeFederationException exception = (BigLakeFederationException) throwable;
                assertThat(exception.category())
                    .isEqualTo(BigLakeFailureCategory.QUOTA_PROJECT_CONFIGURATION);
                assertThat(exception.retryCount()).isZero();
              });
    }

    assertThat(attempts).hasValue(1);
  }

  @Test
  void sanitizeRemoteDetailRedactsCommonSecrets() {
    String sanitized =
        BigLakeFederatedRestClient.sanitizeRemoteDetail(
            "Authorization: Bearer test-token?access_token=abc&client_secret=def");

    assertThat(sanitized).doesNotContain("test-token").doesNotContain("abc").doesNotContain("def");
    assertThat(sanitized).contains("<redacted>");
  }

  private static void invokeConfigRequest(BigLakeFederatedRestClient client) {
    client.get(
        "/v1/config",
        Map.of(),
        TestResponse.class,
        Map.of(),
        error -> {
          throw new RuntimeException(error.message());
        });
  }

  private static BigLakeFederatedRestClient httpBackedClient(HttpTestServer server, int maxRetries) {
    RESTClient delegate =
        HTTPClient.builder(Map.of("rest.client.max-retries", Integer.toString(maxRetries)))
            .uri(server.getUri().toString())
            .withAuthSession(AuthSession.EMPTY)
            .build();
    return new BigLakeFederatedRestClient(
        delegate,
        new SimpleMeterRegistry(),
        "local-catalog",
        Map.of(
            org.apache.iceberg.CatalogProperties.URI,
            server.getUri().toString(),
            "rest.client.max-retries",
            Integer.toString(maxRetries)));
  }

  private static void sendErrorResponse(
      HttpExchange exchange,
      int status,
      String type,
      String message,
      String requestId,
      String retryAfter)
      throws IOException {
    byte[] body =
        String.format(
                "{\"error\":{\"message\":\"%s\",\"type\":\"%s\",\"code\":%d}}",
                message, type, status)
            .getBytes(StandardCharsets.UTF_8);

    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.getResponseHeaders().add("x-goog-request-id", requestId);
    if (retryAfter != null) {
      exchange.getResponseHeaders().add("Retry-After", retryAfter);
    }
    exchange.sendResponseHeaders(status, body.length);
    try (var responseBody = exchange.getResponseBody()) {
      responseBody.write(body);
    }
  }

  private static final class FakeRestClient implements RESTClient {
    private TestResponse response;
    private ErrorResponse errorResponse;
    private RuntimeException runtimeFailure;
    private Map<String, String> responseHeaders = Map.of();

    @Override
    public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T delete(
        String path,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T get(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    public <T extends RESTResponse> T get(
        String path,
        Map<String, String> queryParams,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeadersConsumer) {
      responseHeadersConsumer.accept(responseHeaders);
      if (runtimeFailure != null) {
        throw runtimeFailure;
      }
      if (errorResponse != null) {
        errorHandler.accept(errorResponse);
      }
      return responseType.cast(response);
    }

    @Override
    public <T extends RESTResponse> T post(
        String path,
        RESTRequest body,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T extends RESTResponse> T postForm(
        String path,
        Map<String, String> formData,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }

  private static final class TestResponse implements RESTResponse {
    @Override
    public void validate() {}
  }
}
