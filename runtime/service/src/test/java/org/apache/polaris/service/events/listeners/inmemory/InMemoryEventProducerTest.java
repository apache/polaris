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

package org.apache.polaris.service.events.listeners.inmemory;

import static org.apache.polaris.core.entity.PolarisEvent.ResourceType.CATALOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.EventLoopGroup;
import io.quarkus.netty.MainEventLoopGroup;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectSpy;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.apache.polaris.core.entity.PolarisEvent;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(InMemoryEventProducerTest.Profile.class)
class InMemoryEventProducerTest {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("polaris.realm-context.realms", "test1,test2")
          .put("polaris.persistence.type", "relational-jdbc")
          .put("polaris.persistence.auto-bootstrap-types", "relational-jdbc")
          .put("quarkus.datasource.db-kind", "h2")
          .put(
              "quarkus.datasource.jdbc.url",
              "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
          .put("polaris.event-listener.type", "persistence-in-memory")
          .put("polaris.event-listener.persistence-in-memory-buffer.buffer-time", "5s")
          .put("polaris.event-listener.persistence-in-memory-buffer.max-buffer-size", "10")
          .put(
              "quarkus.fault-tolerance.\"org.apache.polaris.service.events.listeners.inmemory.InMemoryEventListener/flush\".retry.max-retries",
              "1")
          .put(
              "quarkus.fault-tolerance.\"org.apache.polaris.service.events.listeners.inmemory.InMemoryEventListener/flush\".retry.delay",
              "10")
          .build();
    }
  }

  // A delay shorter than the full 5s buffer timeout
  private static final Duration SHORT_DELAY = Duration.ofSeconds(4);

  // A delay longer than the full 5s buffer timeout
  public static final Duration LONG_DELAY = Duration.ofSeconds(8);

  @Inject
  @Identifier("persistence-in-memory")
  InMemoryEventListener producer;

  @InjectSpy
  @Identifier("relational-jdbc")
  @SuppressWarnings("CdiInjectionPointsInspection")
  MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  @MainEventLoopGroup
  @SuppressWarnings("CdiInjectionPointsInspection")
  EventLoopGroup eventLoopGroup;

  @Inject Instance<DataSource> dataSource;

  @Test
  void testFlushOnSize() {
    sendAsync("test1", 10);
    sendAsync("test2", 10);
    assertRows("test1", 10, SHORT_DELAY);
    assertRows("test2", 10, SHORT_DELAY);
  }

  @Test
  void testFlushOnTimeout() {
    sendAsync("test1", 5);
    sendAsync("test2", 5);
    assertRows("test1", 5, LONG_DELAY);
    assertRows("test2", 5, LONG_DELAY);
  }

  @Test
  void testFlushOnShutdown() {
    producer.processEvent("test1", event());
    producer.processEvent("test2", event());
    producer.shutdown();
    assertRows("test1", 1, SHORT_DELAY);
    assertRows("test2", 1, SHORT_DELAY);
  }

  @Test
  void testFailureRecovery() {
    var manager = Mockito.mock(PolarisMetaStoreManager.class);
    doReturn(manager).when(metaStoreManagerFactory).getOrCreateMetaStoreManager(any());
    RuntimeException error = new RuntimeException("error");
    doThrow(error)
        .doThrow(error) // first batch will give up after 2 attempts
        .doThrow(error)
        .doCallRealMethod() // second batch will succeed on the 2nd attempt
        .when(manager)
        .writeEvents(any(), any());
    sendAsync("test1", 20);
    assertRows("test1", 10, SHORT_DELAY);
  }

  @AfterEach
  void clearEvents() throws Exception {
    reset(metaStoreManagerFactory);
    producer.shutdown();
    try (Connection connection = dataSource.get().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM polaris_schema.events");
    }
  }

  private void sendAsync(String realmId, int n) {
    for (int i = 0; i < n; i++) {
      eventLoopGroup.next().execute(() -> producer.processEvent(realmId, event()));
    }
  }

  private void assertRows(String realmId, int expected, Duration timeout) {
    String query = "SELECT COUNT(*) FROM polaris_schema.events WHERE realm_id = '" + realmId + "'";
    await()
        .atMost(timeout)
        .untilAsserted(
            () -> {
              try (Connection connection = dataSource.get().getConnection();
                  Statement statement = connection.createStatement();
                  ResultSet rs = statement.executeQuery(query)) {
                rs.next();
                assertThat(rs.getInt(1)).isEqualTo(expected);
              }
            });
  }

  private static PolarisEvent event() {
    String id = UUID.randomUUID().toString();
    return new PolarisEvent("test", id, null, "test", 0, null, CATALOG, "test");
  }
}
