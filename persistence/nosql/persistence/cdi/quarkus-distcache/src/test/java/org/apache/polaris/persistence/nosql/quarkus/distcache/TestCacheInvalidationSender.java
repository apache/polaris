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
package org.apache.polaris.persistence.nosql.quarkus.distcache;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference.cacheInvalidationEvictReference;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.cacheInvalidations;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidation;
import org.apache.polaris.persistence.nosql.api.cache.DistributedCacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.ObjRef;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheInvalidationSender {
  private static final ObjRef SOME_OBJ_REF = ObjRef.objRef("foo", 1234);

  @InjectSoftAssertions protected SoftAssertions soft;

  protected Vertx vertx;

  @BeforeEach
  void setUp() {
    vertx = Vertx.builder().build();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      vertx.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
    } finally {
      vertx = null;
    }
  }

  @Test
  public void serviceNameLookupFailure() {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var config =
        buildConfig(
            tokens,
            Optional.of(singletonList("serviceName")),
            Duration.ofSeconds(10),
            Duration.ofSeconds(10));

    soft.assertThatThrownBy(
            () ->
                new CacheInvalidationSender(vertx, config, 80, senderId) {
                  @Override
                  Future<List<String>> resolveServiceNames(List<String> serviceNames) {
                    return failedFuture(new RuntimeException("foo"));
                  }

                  @Override
                  List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
                      List<CacheInvalidation> batch, List<String> resolvedAddresses) {
                    soft.fail("Not expected");
                    return null;
                  }
                })
        .hasMessage("Failed to resolve service names [serviceName] for remote cache invalidations")
        .cause()
        .hasMessage("foo");
  }

  @Test
  public void regularServiceNameLookups() throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var config =
        buildConfig(
            tokens,
            Optional.of(singletonList("serviceName")),
            Duration.ofMillis(1),
            Duration.ofSeconds(10));

    var resolveSemaphore = new Semaphore(1);
    var continueSemaphore = new Semaphore(0);
    var submittedSemaphore = new Semaphore(0);
    var updateResolvedSemaphore = new Semaphore(0);
    var currentAddresses = List.of("127.1.1.1");
    var resolveResult = new AtomicReference<>(succeededFuture(currentAddresses));
    var submitResolvedAddresses = new AtomicReference<List<String>>();

    try {
      CacheInvalidationSender sender =
          new CacheInvalidationSender(vertx, config, 80, senderId) {
            @Override
            Future<List<String>> resolveServiceNames(List<String> serviceNames) {
              try {
                assertThat(resolveSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              try {
                return resolveResult.get();
              } finally {
                continueSemaphore.release();
              }
            }

            @Override
            void updateResolvedAddresses(List<String> all) {
              try {
                super.updateResolvedAddresses(all);
              } finally {
                updateResolvedSemaphore.release();
              }
            }

            @Override
            List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
                List<CacheInvalidation> batch, List<String> resolvedAddresses) {
              submitResolvedAddresses.set(resolvedAddresses);
              submittedSemaphore.release();
              return null;
            }
          };

      // "consume" after initial, blocking call to resolveServiceNames() from the constructor
      assertThat(continueSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      assertThat(updateResolvedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();

      // Send an invalidation, compare addresses
      sender.evictObj("repo", SOME_OBJ_REF);
      assertThat(submittedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      soft.assertThat(submitResolvedAddresses.get())
          .containsExactlyInAnyOrderElementsOf(currentAddresses);

      // simulate change of resolved addresses
      currentAddresses = List.of("127.2.2.2", "127.3.3.3");
      resolveResult.set(succeededFuture(currentAddresses));
      resolveSemaphore.release();
      // wait until next call to resolveServiceNames() has been triggered
      assertThat(continueSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      assertThat(updateResolvedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();

      // Send another invalidation, compare addresses
      sender.evictObj("repo", SOME_OBJ_REF);
      assertThat(submittedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      soft.assertThat(submitResolvedAddresses.get())
          .containsExactlyInAnyOrderElementsOf(currentAddresses);

      // simulate a failure resolving the addresses
      resolveResult.set(failedFuture(new RuntimeException("blah")));
      resolveSemaphore.release();
      // wait until next call to resolveServiceNames() has been triggered
      assertThat(continueSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();

      // Send another invalidation, compare addresses
      sender.evictObj("repo", SOME_OBJ_REF);
      assertThat(submittedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      soft.assertThat(submitResolvedAddresses.get())
          .containsExactlyInAnyOrderElementsOf(currentAddresses);

      // simulate another change of resolved addresses
      currentAddresses = List.of("127.4.4.4", "127.5.5.5");
      resolveResult.set(succeededFuture(currentAddresses));
      resolveSemaphore.release();
      // wait until next call to resolveServiceNames() has been triggered
      assertThat(continueSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      assertThat(updateResolvedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();

      // Send another invalidation, compare addresses
      sender.evictObj("repo", SOME_OBJ_REF);
      assertThat(submittedSemaphore.tryAcquire(30, TimeUnit.SECONDS)).isTrue();
      soft.assertThat(submitResolvedAddresses.get())
          .containsExactlyInAnyOrderElementsOf(currentAddresses);
    } finally {
      // Permit a lot, the test might otherwise "hang" in resolveServiceNames()
      resolveSemaphore.release(10_000_000);
    }
  }

  @Test
  public void noServiceNames() throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var config =
        buildConfig(tokens, Optional.empty(), Duration.ofSeconds(10), Duration.ofSeconds(10));

    var sender =
        new CacheInvalidationSender(vertx, config, 80, senderId) {
          @Override
          Future<List<String>> resolveServiceNames(List<String> serviceNames) {
            return succeededFuture(List.of());
          }

          @Override
          List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
              List<CacheInvalidation> batch, List<String> resolvedAddresses) {
            soft.fail("Not expected");
            return null;
          }
        };

    var senderSpy = spy(sender);

    senderSpy.evictObj("repo", SOME_OBJ_REF);

    // Hard to test that nothing is done, if the list of resolved addresses is empty, but the
    // condition is easy. If this tests is flaky, then there's something broken.
    Thread.sleep(100L);

    verify(senderSpy).evictObj("repo", SOME_OBJ_REF);
    verify(senderSpy).enqueue(cacheInvalidationEvictObj("repo", SOME_OBJ_REF));
    verifyNoMoreInteractions(senderSpy);
  }

  @ParameterizedTest
  @MethodSource("invalidations")
  public void mockedSendSingleInvalidation(
      Consumer<DistributedCacheInvalidation> invalidation, CacheInvalidation expected)
      throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var serviceNames = singletonList("service-name");
    var resolvedServiceNames = singletonList("service-name-resolved");

    var config =
        buildConfig(
            tokens, Optional.of(serviceNames), Duration.ofSeconds(10), Duration.ofSeconds(10));

    var sem = new Semaphore(0);
    var sender =
        new CacheInvalidationSender(vertx, config, 80, senderId) {
          @Override
          Future<List<String>> resolveServiceNames(List<String> serviceNames) {
            return succeededFuture(resolvedServiceNames);
          }

          @Override
          List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
              List<CacheInvalidation> batch, List<String> resolvedAddresses) {
            sem.release(1);
            return null;
          }
        };

    var senderSpy = spy(sender);

    invalidation.accept(senderSpy);
    assertThat(sem.tryAcquire(30, TimeUnit.SECONDS)).isTrue();

    verify(senderSpy).submit(singletonList(expected), resolvedServiceNames);
  }

  @Test
  public void mockedAllInvalidationTypes() throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var serviceNames = singletonList("service-name");
    var resolvedServiceNames = singletonList("service-name-resolved");

    var config =
        buildConfig(
            tokens, Optional.of(serviceNames), Duration.ofSeconds(10), Duration.ofSeconds(10));

    var sem = new Semaphore(0);
    var received = new ConcurrentLinkedQueue<>();
    var sender =
        new CacheInvalidationSender(vertx, config, 80, senderId) {
          @Override
          Future<List<String>> resolveServiceNames(List<String> serviceNames) {
            return succeededFuture(resolvedServiceNames);
          }

          @Override
          List<Future<Map.Entry<HttpClientResponse, Buffer>>> submit(
              List<CacheInvalidation> batch, List<String> resolvedAddresses) {
            received.addAll(batch);
            soft.assertThat(resolvedAddresses)
                .containsExactlyInAnyOrderElementsOf(resolvedServiceNames);
            sem.release(batch.size());
            return null;
          }
        };

    var senderSpy = spy(sender);

    var expected =
        invalidations().map(args -> args.get()[1]).map(CacheInvalidation.class::cast).toList();

    invalidations()
        .map(args -> args.get()[0])
        .map(
            i -> {
              @SuppressWarnings({"UnnecessaryLocalVariable", "unchecked"})
              Consumer<DistributedCacheInvalidation> r = (Consumer<DistributedCacheInvalidation>) i;
              return r;
            })
        .forEach(i -> i.accept(senderSpy));

    assertThat(sem.tryAcquire(expected.size(), 30, TimeUnit.SECONDS)).isTrue();

    soft.assertThat(received).containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest
  @MethodSource("invalidations")
  public void sendSingleInvalidation(
      @SuppressWarnings("unused") Consumer<DistributedCacheInvalidation> invalidation,
      CacheInvalidation expected)
      throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var serviceNames = singletonList("service-name");

    var config =
        buildConfig(
            tokens, Optional.of(serviceNames), Duration.ofSeconds(10), Duration.ofSeconds(10));

    var mapper = new ObjectMapper();

    var body = new AtomicReference<String>();
    var reqUri = new AtomicReference<URI>();
    try (var receiver =
        new HttpTestServer(
            config.cacheInvalidationUri(),
            exchange -> {
              try (InputStream requestBody = exchange.getRequestBody()) {
                body.set(new String(requestBody.readAllBytes(), UTF_8));
              }
              reqUri.set(exchange.getRequestURI());
              exchange.sendResponseHeaders(204, 0);
              exchange.getResponseBody().close();
            })) {

      var uri = receiver.getUri();

      var sender =
          new CacheInvalidationSender(vertx, config, uri.getPort(), senderId) {
            @Override
            Future<List<String>> resolveServiceNames(List<String> serviceNames) {
              return succeededFuture(List.of(uri.getHost()));
            }
          };

      var future =
          CompletableFuture.allOf(
              sender.submit(singletonList(expected), singletonList(uri.getHost())).stream()
                  .map(Future::toCompletionStage)
                  .map(CompletionStage::toCompletableFuture)
                  .toArray(CompletableFuture[]::new));

      soft.assertThat(future).succeedsWithin(30, TimeUnit.SECONDS);

      soft.assertThat(body.get())
          .isEqualTo(mapper.writeValueAsString(cacheInvalidations(singletonList(expected))));
      soft.assertThat(reqUri.get()).extracting(URI::getPath).isEqualTo("/foo/bar/");
      soft.assertThat(reqUri.get())
          .extracting(URI::getQuery)
          .isEqualTo("sender=" + senderId.instanceId());
    }
  }

  @Test
  public void allInvalidationTypes() throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var serviceNames = singletonList("service-name");

    var config =
        buildConfig(
            tokens, Optional.of(serviceNames), Duration.ofSeconds(10), Duration.ofSeconds(30));

    var expected =
        invalidations().map(args -> args.get()[1]).map(CacheInvalidation.class::cast).toList();

    var mapper = new ObjectMapper();

    var body = new AtomicReference<String>();
    var reqUri = new AtomicReference<URI>();
    try (HttpTestServer receiver =
        new HttpTestServer(
            config.cacheInvalidationUri(),
            exchange -> {
              try (InputStream requestBody = exchange.getRequestBody()) {
                body.set(new String(requestBody.readAllBytes(), UTF_8));
              }
              reqUri.set(exchange.getRequestURI());
              exchange.sendResponseHeaders(204, 0);
              exchange.getResponseBody().close();
            })) {

      var uri = receiver.getUri();

      var sender =
          new CacheInvalidationSender(vertx, config, uri.getPort(), senderId) {
            @Override
            Future<List<String>> resolveServiceNames(List<String> serviceNames) {
              return succeededFuture(List.of(uri.getHost()));
            }
          };

      var future =
          Future.all(sender.submit(expected, singletonList(uri.getHost())))
              .toCompletionStage()
              .toCompletableFuture();

      soft.assertThat(future).succeedsWithin(30, TimeUnit.SECONDS);

      soft.assertThat(body.get())
          .isEqualTo(mapper.writeValueAsString(cacheInvalidations(expected)));
      soft.assertThat(reqUri.get()).extracting(URI::getPath).isEqualTo("/foo/bar/");
      soft.assertThat(reqUri.get())
          .extracting(URI::getQuery)
          .isEqualTo("sender=" + senderId.instanceId());
    }
  }

  @Test
  public void sendInvalidationTimeout() throws Exception {
    var senderId = ServerInstanceId.of("senderId");

    var token = "token";
    var tokens = singletonList(token);

    var serviceNames = singletonList("service-name");

    var config =
        buildConfig(
            tokens, Optional.of(serviceNames), Duration.ofSeconds(10), Duration.ofMillis(1));

    var expected =
        invalidations().map(args -> args.get()[1]).map(CacheInvalidation.class::cast).toList();

    try (var receiver =
        new HttpTestServer(
            config.cacheInvalidationUri(),
            exchange -> {
              try (InputStream requestBody = exchange.getRequestBody()) {
                requestBody.readAllBytes();
              }
              // don't send a response -> provoke a timeout
              exchange.getResponseBody().close();
            })) {

      var uri = receiver.getUri();

      var sender =
          new CacheInvalidationSender(vertx, config, uri.getPort(), senderId) {
            @Override
            Future<List<String>> resolveServiceNames(List<String> serviceNames) {
              return succeededFuture(List.of(uri.getHost()));
            }
          };

      var future =
          CompletableFuture.allOf(
              sender.submit(expected, singletonList(uri.getHost())).stream()
                  .map(Future::toCompletionStage)
                  .map(CompletionStage::toCompletableFuture)
                  .toArray(CompletableFuture[]::new));

      soft.assertThat(future)
          .failsWithin(30, TimeUnit.SECONDS)
          .withThrowableOfType(ExecutionException.class)
          .withMessageContaining("Timeout 1 (ms) fired");
    }
  }

  static Stream<Arguments> invalidations() {
    return Stream.of(
        arguments(
            (Consumer<DistributedCacheInvalidation>) i -> i.evictObj("repo", SOME_OBJ_REF),
            cacheInvalidationEvictObj("repo", SOME_OBJ_REF)),
        arguments(
            (Consumer<DistributedCacheInvalidation>) i -> i.evictReference("repo", "refs/foo/bar"),
            cacheInvalidationEvictReference("repo", "refs/foo/bar")));
  }

  private static QuarkusDistributedCacheInvalidationsConfig buildConfig(
      List<String> tokens,
      Optional<List<String>> serviceName,
      Duration interval,
      Duration requestTimeout) {
    var config = mock(QuarkusDistributedCacheInvalidationsConfig.class);
    when(config.cacheInvalidationValidTokens()).thenReturn(Optional.of(tokens));
    when(config.cacheInvalidationServiceNames()).thenReturn(serviceName);
    when(config.cacheInvalidationServiceNameLookupInterval()).thenReturn(interval);
    when(config.cacheInvalidationBatchSize()).thenReturn(10);
    when(config.cacheInvalidationUri()).thenReturn("/foo/bar/");
    when(config.cacheInvalidationRequestTimeout()).thenReturn(Optional.of(requestTimeout));
    return config;
  }
}
