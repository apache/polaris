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
package org.apache.polaris.service.distcache;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictObj.cacheInvalidationEvictObj;
import static org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidationEvictReference.cacheInvalidationEvictReference;
import static org.apache.polaris.persistence.nosql.api.obj.ObjRef.objRef;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.api.SystemPersistence;
import org.apache.polaris.persistence.nosql.api.cache.CacheBackend;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations;
import org.apache.polaris.persistence.nosql.api.cache.CacheInvalidations.CacheInvalidation;
import org.apache.polaris.persistence.nosql.api.obj.SimpleTestObj;
import org.apache.polaris.service.catalog.iceberg.AbstractIcebergCatalogTest;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

@QuarkusTest
@TestProfile(TestPersistenceDistCacheInvalidationsIntegration.Profile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
// Need two IPs in this test. macOS doesn't allow binding to arbitrary 127.x.x.x addresses.
@EnabledOnOs(OS.LINUX)
@SuppressWarnings("CdiInjectionPointsInspection")
public class TestPersistenceDistCacheInvalidationsIntegration {

  public static class Profile extends AbstractIcebergCatalogTest.Profile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("quarkus.management.port", "0")
          .put("quarkus.management.host", "127.0.0.1")
          .put("quarkus.management.enabled", "true")
          .put("polaris.persistence.type", "nosql")
          .put("polaris.persistence.auto-bootstrap-types", "nosql")
          .put("polaris.persistence.nosql.backend", "InMemory")
          .put(
              "polaris.persistence.distributed-cache-invalidations.valid-tokens", "token1," + TOKEN)
          .put(
              "polaris.persistence.distributed-cache-invalidations.service-names",
              "=127.0.0.1,=127.1.2.3")
          .build();
    }
  }

  static final String TOKEN = "otherToken";
  static final String ENDPOINT = "/polaris-management/cache-coherency";

  SoftAssertions soft;
  ObjectMapper mapper;

  @Inject CacheBackend cacheBackend;

  @Inject @SystemPersistence Persistence systemPersistence;
  @Inject RealmPersistenceFactory realmPersistenceFactory;

  @BeforeEach
  public void before() {
    soft = new SoftAssertions();
    mapper = new ObjectMapper();
  }

  @AfterEach
  public void after() {
    soft.assertAll();
  }

  private static int quarkusManagementPort() {
    var quarkusManagementPort =
        ConfigProvider.getConfig().getConfigValue("quarkus.management.port");
    var managementPortString = quarkusManagementPort.getValue();
    return Integer.parseInt(managementPortString);
  }

  private static URI invalidationEndpoint() {
    return URI.create(
        format(
            "http://127.0.0.1:%d%s?sender=" + UUID.randomUUID(),
            quarkusManagementPort(),
            ENDPOINT));
  }

  /**
   * This one is not a real test, but a necessity due to the set environment setup.
   *
   * <p>In this test, we can no longer use a statically configured management-port but have to use a
   * dynamically bound one. That forces the {@code
   * org.apache.polaris.persistence.nosql.quarkus.distcache.CacheInvalidationSender} to queue
   * invalidation messages until the actual bound management port is available from the
   * configuration.
   *
   * <p>This test-code effectively just drains the queue until it is empty, so that the actual tests
   * have a clean state.
   */
  @Test
  @Order(1)
  public void clearQueuedInvalidations() throws Exception {
    var queue = new LinkedBlockingQueue<Map.Entry<URI, String>>();
    try (var ignore =
        new HttpTestServer(
            new InetSocketAddress("127.1.2.3", quarkusManagementPort()),
            ENDPOINT,
            exchange -> {
              try (InputStream requestBody = exchange.getRequestBody()) {
                queue.add(
                    entry(exchange.getRequestURI(), new String(requestBody.readAllBytes(), UTF_8)));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              exchange.sendResponseHeaders(204, -1);
              exchange.getResponseBody().close();
            })) {
      var persistence = realmPersistenceFactory.newBuilder().realmId("warmup").build();
      var obj = SimpleTestObj.builder().id(persistence.generateId()).text("warmup").build();
      var expected = cacheInvalidationEvictObj(persistence.realmId(), objRef(obj));

      persistence.write(obj, SimpleTestObj.class);

      awaitContainsInvalidation(queue, expected);
    }
  }

  @Test
  @Order(2)
  public void systemRealm() throws Exception {
    sendReceive(systemPersistence);
  }

  @Test
  @Order(2)
  public void otherRealm() throws Exception {
    var persistence = realmPersistenceFactory.newBuilder().realmId("foo").build();
    sendReceive(persistence);
  }

  private void sendReceive(Persistence persistence) throws Exception {
    var queue = new LinkedBlockingQueue<Map.Entry<URI, String>>();
    try (var ignore =
        new HttpTestServer(
            new InetSocketAddress("127.1.2.3", quarkusManagementPort()),
            ENDPOINT,
            exchange -> {
              try (InputStream requestBody = exchange.getRequestBody()) {
                queue.add(
                    entry(exchange.getRequestURI(), new String(requestBody.readAllBytes(), UTF_8)));
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              exchange.sendResponseHeaders(204, -1);
              exchange.getResponseBody().close();
            })) {

      var obj = SimpleTestObj.builder().id(persistence.generateId()).text("test").build();
      persistence.write(obj, SimpleTestObj.class);

      // verify that "we" received the invalidation for the obj
      var invalidation = queue.poll(1, TimeUnit.MINUTES);
      var uri = requireNonNull(invalidation).getKey();
      soft.assertThat(uri.getRawQuery()).startsWith("sender=");
      var invalidations = mapper.readValue(invalidation.getValue(), CacheInvalidations.class);
      soft.assertThat(invalidations)
          .extracting(CacheInvalidations::invalidations, list(CacheInvalidation.class))
          .containsExactly(cacheInvalidationEvictObj(persistence.realmId(), objRef(obj)));

      // verify that "the service" processes an invalidation
      soft.assertThat(cacheBackend.get(persistence.realmId(), objRef(obj))).isNotNull();
      send(invalidations);
      awaitCondition(
          () -> assertThat(cacheBackend.get(persistence.realmId(), objRef(obj))).isNull());

      // reference

      var refName = "foo-ref";
      persistence.createReference(refName, Optional.empty());

      // verify that "we" received the invalidation for the reference
      invalidation = queue.poll(1, TimeUnit.MINUTES);
      uri = requireNonNull(invalidation).getKey();
      soft.assertThat(uri.getRawQuery()).startsWith("sender=");
      invalidations = mapper.readValue(invalidation.getValue(), CacheInvalidations.class);
      soft.assertThat(invalidations)
          .extracting(CacheInvalidations::invalidations, list(CacheInvalidation.class))
          .containsExactly(cacheInvalidationEvictReference(persistence.realmId(), refName));

      // verify that "the service" processes an invalidation
      soft.assertThat(cacheBackend.getReference(persistence.realmId(), refName)).isNotNull();
      send(invalidations);
      awaitCondition(
          () -> assertThat(cacheBackend.get(persistence.realmId(), objRef(obj))).isNull());
    }
  }

  @SuppressWarnings("BusyWait")
  private void awaitCondition(Runnable test) throws Exception {
    var tEnd = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    while (System.nanoTime() < tEnd) {
      try {
        test.run();
        return;
      } catch (AssertionError e) {
        if (System.nanoTime() > tEnd) {
          throw e;
        }
      }
      Thread.sleep(1);
    }
  }

  private void awaitContainsInvalidation(
      LinkedBlockingQueue<Map.Entry<URI, String>> queue, CacheInvalidation expected)
      throws Exception {
    var tEnd = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    while (System.nanoTime() < tEnd) {
      var invalidation = queue.poll(1, TimeUnit.SECONDS);
      if (invalidation == null) {
        continue;
      }

      var invalidations = mapper.readValue(invalidation.getValue(), CacheInvalidations.class);
      if (invalidations.invalidations().contains(expected)) {
        return;
      }
    }

    throw new AssertionError("Timed out waiting for warmup invalidation " + expected);
  }

  private void send(CacheInvalidations invalidations) throws Exception {
    var urlConn = invalidationEndpoint().toURL().openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestProperty("Content-Type", APPLICATION_JSON);
    urlConn.setRequestProperty("Polaris-Cache-Invalidation-Token", TOKEN);
    try (var out = urlConn.getOutputStream()) {
      out.write(mapper.writeValueAsBytes(invalidations));
    }
    urlConn.getInputStream().readAllBytes();
  }
}
