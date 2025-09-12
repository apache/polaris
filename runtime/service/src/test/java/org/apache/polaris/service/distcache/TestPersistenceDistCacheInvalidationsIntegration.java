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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

@QuarkusTest
@TestProfile(TestPersistenceDistCacheInvalidationsIntegration.Profile.class)
// Need two IPs in this test. macOS doesn't allow binding to arbitrary 127.x.x.x addresses.
@EnabledOnOs(OS.LINUX)
@SuppressWarnings("CdiInjectionPointsInspection")
public class TestPersistenceDistCacheInvalidationsIntegration {

  public static class Profile extends AbstractIcebergCatalogTest.Profile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("quarkus.management.port", "" + QUARKUS_MANAGEMENT_PORT)
          .put("quarkus.management.test-port", "" + QUARKUS_MANAGEMENT_PORT)
          .put("quarkus.management.host", "127.0.0.1")
          .put("quarkus.management.enabled", "true")
          .put("polaris.persistence.type", "nosql")
          .put("polaris.persistence.backend.type", "InMemory")
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

  // MUST be constant for test AND service
  static final int QUARKUS_MANAGEMENT_PORT = 64321;

  static URI CACHE_INVALIDATIONS_ENDPOINT =
      URI.create(
          format(
              "http://127.0.0.1:%d%s?sender=" + UUID.randomUUID(),
              QUARKUS_MANAGEMENT_PORT,
              ENDPOINT));

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

  @Test
  public void systemRealm() throws Exception {
    sendReceive(systemPersistence);
  }

  @Test
  public void otherRealm() throws Exception {
    var persistence = realmPersistenceFactory.newBuilder().realmId("foo").build();
    sendReceive(persistence);
  }

  private void sendReceive(Persistence persistence) throws Exception {
    var queue = new LinkedBlockingQueue<Map.Entry<URI, String>>();
    try (var ignore =
        new HttpTestServer(
            new InetSocketAddress("127.1.2.3", QUARKUS_MANAGEMENT_PORT),
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

  private void send(CacheInvalidations invalidations) throws Exception {
    var urlConn = CACHE_INVALIDATIONS_ENDPOINT.toURL().openConnection();
    urlConn.setDoOutput(true);
    urlConn.setRequestProperty("Content-Type", APPLICATION_JSON);
    urlConn.setRequestProperty("Polaris-Cache-Invalidation-Token", TOKEN);
    try (var out = urlConn.getOutputStream()) {
      out.write(mapper.writeValueAsBytes(invalidations));
    }
    urlConn.getInputStream().readAllBytes();
  }
}
