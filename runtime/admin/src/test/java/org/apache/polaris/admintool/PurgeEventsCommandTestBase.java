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
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.polaris.admintool.config.QuarkusPersistenceConfiguration;
import org.apache.polaris.core.entity.EventEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
public abstract class PurgeEventsCommandTestBase {
  private static final String REALM1 = "purge-events-test-realm1";
  private static final String REALM2 = "purge-events-test-realm2";

  /**
   * Seeds events on every application start. This bean is global to the test application, so it
   * also runs for other test classes; it only writes events where they are supported. It is a
   * separate bean, rather than an observer on this class, so that it runs once per start and not
   * once per subclass.
   */
  @Singleton
  static class EventsSeeder {
    void seedEvents(
        @Observes StartupEvent event,
        QuarkusPersistenceConfiguration persistenceConfiguration,
        MetaStoreManagerFactory metaStoreManagerFactory) {
      if (!"relational-jdbc".equals(persistenceConfiguration.type())) {
        return;
      }
      metaStoreManagerFactory.bootstrapRealms(List.of(REALM1, REALM2), RootCredentialsSet.EMPTY);
      long now = System.currentTimeMillis();
      long old = now - Duration.ofDays(10).toMillis();
      metaStoreManagerFactory
          .getOrCreateSession(() -> REALM1)
          .writeEvents(
              LongStream.of(old, old, old, now)
                  .mapToObj(PurgeEventsCommandTestBase::event)
                  .toList());
      metaStoreManagerFactory
          .getOrCreateSession(() -> REALM2)
          .writeEvents(List.of(event(old), event(now)));
    }
  }

  private static EventEntity event(long timestampMs) {
    return new EventEntity(
        null,
        UUID.randomUUID().toString(),
        null,
        "AFTER_CREATE_PRINCIPAL",
        timestampMs,
        "root",
        EventEntity.ResourceType.REALM,
        "principal-1");
  }

  @Test
  public void testPurgeEvents(QuarkusMainLauncher launcher) {
    String[] args = {
      "purge-events", "-r", REALM1, "-r", REALM2, "--older-than", "P1D", "--batch-size", "2"
    };
    // The first run removes the old events seeded by any earlier application start in this
    // database. The second run then sees exactly the old events seeded by its own start.
    assertThat(launcher.launch(args).exitCode()).isEqualTo(0);
    LaunchResult result = launcher.launch(args);
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .containsPattern("Purged 3 events older than \\S+ from realm " + REALM1)
        .containsPattern("Purged 1 events older than \\S+ from realm " + REALM2)
        .contains("Purge of events completed successfully.");
  }

  @Test
  @Launch(
      value = {"purge-events", "-r", REALM1, "--older-than", "PT0S"},
      exitCode = BaseCommand.EXIT_CODE_USAGE)
  public void testZeroOlderThan(LaunchResult result) {
    assertThat(result.getErrorOutput()).contains("--older-than must be a positive duration.");
  }
}
