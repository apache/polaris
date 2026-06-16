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
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
public abstract class PurgeCommandTestBase {
  private static final String REALM1 = "purge-test-realm1";
  private static final String REALM2 = "purge-test-realm2";
  private static final String MISSING_REALM = "purge-test-missing-realm";

  protected SoftAssertions soft;

  @BeforeEach
  void setup() {
    soft = new SoftAssertions();
  }

  @AfterEach
  void after() {
    soft.assertAll();
  }

  void preBootstrap(@Observes StartupEvent event, MetaStoreManagerFactory metaStoreManagerFactory) {
    metaStoreManagerFactory.bootstrapRealms(List.of(REALM1, REALM2), RootCredentialsSet.EMPTY);
  }

  @Test
  @Launch(value = {"purge", "-r", REALM1, "-r", REALM2})
  public void testPurge(LaunchResult result) {
    assertThat(result.getOutput()).contains("Purge completed successfully.");
  }

  @Test
  @Launch(
      value = {"purge", "-r", MISSING_REALM},
      exitCode = BaseCommand.EXIT_CODE_PURGE_ERROR)
  public void testPurgeFailure(LaunchResult result) {
    soft.assertThat(result.getOutput())
        .contains(
            "Realm "
                + MISSING_REALM
                + " is not bootstrapped, could not load root principal. Please run Bootstrap command.");
    soft.assertThat(result.getErrorOutput()).contains("Purge encountered errors during operation.");
  }
}
