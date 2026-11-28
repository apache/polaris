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
package org.apache.polaris.admintool.nosql;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import jakarta.enterprise.event.Observes;
import java.util.List;
import org.apache.polaris.admintool.AdminProfiles;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
@TestProfile(AdminProfiles.NoSqlInMemory.class)
class NoSqlInMemoryBootstrapPurgingRealmTest {

  private static final String REALM = "bootstrap-purging-realm";

  void seedPurgingRealm(
      @Observes StartupEvent event, MetaStoreManagerFactory metaStoreManagerFactory) {
    metaStoreManagerFactory.bootstrapRealms(List.of(REALM), RootCredentialsSet.EMPTY);
    metaStoreManagerFactory.purgeRealms(List.of(REALM));
  }

  @Test
  public void testBootstrapOfPurgingRealmIsReportedAndFails(QuarkusMainLauncher launcher) {
    LaunchResult result = launcher.launch("bootstrap", "-r", REALM, "--print-credentials");

    assertThat(result.exitCode()).isNotEqualTo(0);
    assertThat(result.getErrorOutput())
        .contains("Bootstrapping '" + REALM + "' failed")
        .contains("PURGING")
        .contains("realm id cannot be reused");
    assertThat(result.getOutput())
        .doesNotContain("successfully bootstrapped")
        .doesNotContain("Bootstrap completed successfully.")
        .doesNotContain("root principal credentials");
  }
}
