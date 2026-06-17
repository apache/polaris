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

import static org.apache.polaris.core.entity.EventEntity.ResourceType.CATALOG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.reset;

import io.agroal.api.AgroalDataSource;
import io.netty.channel.EventLoopGroup;
import io.quarkus.agroal.DataSource;
import io.quarkus.netty.MainEventLoopGroup;
import io.quarkus.test.junit.mockito.InjectSpy;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

abstract class InMemoryBufferEventListenerTestBase {

  @Inject
  @Identifier("persistence-in-memory-buffer")
  Instance<InMemoryBufferEventListener> producerInstance;

  InMemoryBufferEventListener producer;

  @InjectSpy
  @Identifier("relational-jdbc")
  @SuppressWarnings("CdiInjectionPointsInspection")
  MetaStoreManagerFactory metaStoreManagerFactory;

  @Inject
  @MainEventLoopGroup
  @SuppressWarnings("CdiInjectionPointsInspection")
  EventLoopGroup eventLoopGroup;

  @Inject
  @DataSource("h2")
  Instance<AgroalDataSource> dataSource;

  @BeforeEach
  void resolveProducer() {
    producer = producerInstance.get();
  }

  @AfterEach
  void clearEvents() throws Exception {
    reset(metaStoreManagerFactory);
    producerInstance.destroy(producer);
    try (Connection connection = dataSource.get().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM polaris_schema.events");
    }
  }

  void sendAsync(String realmId, int n) {
    for (int i = 0; i < n; i++) {
      eventLoopGroup.next().execute(() -> producer.processEvent(realmId, event()));
    }
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  void assertRows(String realmId, int expected) {
    String query = "SELECT COUNT(*) FROM polaris_schema.events WHERE realm_id = '" + realmId + "'";
    await()
        .atMost(Duration.ofSeconds(10))
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

  static EventEntity event() {
    String id = UUID.randomUUID().toString();
    return new EventEntity("test", id, null, "test", 0, null, CATALOG, "test");
  }
}
