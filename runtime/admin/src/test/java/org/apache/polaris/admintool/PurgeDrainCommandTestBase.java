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
package org.apache.polaris.admintool;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;
import jakarta.enterprise.event.Observes;
import java.util.List;
import java.util.UUID;
import org.apache.polaris.admintool.config.QuarkusPersistenceConfiguration;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
public abstract class PurgeDrainCommandTestBase {
  private static final String REALM1 = "purge-drain-test-realm1";
  private static final String REALM2 = "purge-drain-test-realm2";
  private static final int EVENTS_PER_REALM = 5;
  private static final String JDBC_PERSISTENCE_TYPE = "relational-jdbc";

  /**
   * Purging a realm queues it into {@code realm_purge}; the launched {@code purge-drain} then
   * drains the queue. Events are written first so the drain issues its real {@code DELETE} against
   * the database under test: were that statement not accepted there, the drain would fail and
   * report no realm drained.
   *
   * <p>A CDI observer declared here is notified on every admin tool startup in this module, not
   * only for this class's subclasses, so writing events is confined to the backend that persists
   * them: the NoSQL backend's {@code writeEvents} throws by design, and an unguarded call would
   * fail the startup of every other admin tool test.
   */
  void preBootstrapWriteEventsAndPurge(
      @Observes StartupEvent event,
      MetaStoreManagerFactory metaStoreManagerFactory,
      QuarkusPersistenceConfiguration persistenceConfiguration) {
    metaStoreManagerFactory.bootstrapRealms(List.of(REALM1, REALM2), RootCredentialsSet.EMPTY);
    if (JDBC_PERSISTENCE_TYPE.equals(persistenceConfiguration.type())) {
      for (String realm : List.of(REALM1, REALM2)) {
        writeEvents(metaStoreManagerFactory, realm);
      }
    }
    metaStoreManagerFactory.purgeRealms(List.of(REALM1, REALM2));
  }

  private static void writeEvents(MetaStoreManagerFactory factory, String realm) {
    RealmContext realmContext = () -> realm;
    var persistence = factory.getOrCreateSession(realmContext);
    for (int i = 0; i < EVENTS_PER_REALM; i++) {
      persistence.writeEvents(
          List.of(
              new EventEntity(
                  "catalog-1",
                  UUID.randomUUID().toString(),
                  "req-" + i,
                  "TEST_EVENT",
                  System.currentTimeMillis(),
                  "principal-1",
                  EventEntity.ResourceType.CATALOG,
                  "resource-1")));
    }
  }

  @Test
  @Launch(value = {"purge-drain"})
  public void testPurgeDrain(LaunchResult result) {
    assertThat(result.getOutput()).contains("Drained 2 realm purge(s).");
  }
}
