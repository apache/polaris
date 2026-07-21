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
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
public abstract class PurgeDrainCommandTestBase {
  private static final String REALM1 = "purge-drain-test-realm1";
  private static final String REALM2 = "purge-drain-test-realm2";

  // Purging a realm queues it into realm_purge; the launched purge-drain then drains the queue.
  void preBootstrapAndPurge(
      @Observes StartupEvent event, MetaStoreManagerFactory metaStoreManagerFactory) {
    metaStoreManagerFactory.bootstrapRealms(List.of(REALM1, REALM2), RootCredentialsSet.EMPTY);
    metaStoreManagerFactory.purgeRealms(List.of(REALM1, REALM2));
  }

  @Test
  @Launch(value = {"purge-drain"})
  public void testPurgeDrain(LaunchResult result) {
    assertThat(result.getOutput()).contains("Drained 2 realm purge(s).");
  }
}
