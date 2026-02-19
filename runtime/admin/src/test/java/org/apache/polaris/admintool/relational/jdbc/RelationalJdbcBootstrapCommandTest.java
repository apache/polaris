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
package org.apache.polaris.admintool.relational.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import org.apache.polaris.admintool.BootstrapCommandTestBase;
import org.junit.jupiter.api.Test;

@TestProfile(RelationalJdbcAdminProfile.class)
public class RelationalJdbcBootstrapCommandTest extends BootstrapCommandTestBase {

  @Test
  public void testBootstrapFailsWhenAddingRealmWithDifferentSchemaVersion(
      QuarkusMainLauncher launcher) {
    // First, bootstrap the schema to version 1
    LaunchResult result1 =
        launcher.launch("bootstrap", "-v", "1", "-r", "realm1", "-c", "realm1,root,s3cr3t");
    assertThat(result1.exitCode()).isEqualTo(0);
    assertThat(result1.getOutput()).contains("Bootstrap completed successfully.");

    // TODO: enable this once we enable postgres container reuse in the same test.
    // LaunchResult result2 = launcher.launch("bootstrap", "-v", "2", "-r", "realm2", "-c",
    // "realm2,root,s3cr3t");
    // assertThat(result2.exitCode()).isEqualTo(EXIT_CODE_BOOTSTRAP_ERROR);
    // assertThat(result2.getOutput()).contains("Cannot bootstrap due to schema version mismatch.");
  }

  @Test
  public void testBootstrapWithIncludeMetrics(QuarkusMainLauncher launcher) {
    // Test that --include-metrics option is accepted and bootstrap completes successfully.
    // The metrics tables are created during bootstrap when this flag is set.
    LaunchResult result =
        launcher.launch(
            "bootstrap", "-r", "realm1", "-c", "realm1,root,s3cr3t", "--include-metrics");
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Realm 'realm1' successfully bootstrapped.")
        .contains("Bootstrap completed successfully.");
  }

  @Test
  public void testBootstrapWithoutIncludeMetrics(QuarkusMainLauncher launcher) {
    // Test that bootstrap works without --include-metrics (default behavior)
    LaunchResult result = launcher.launch("bootstrap", "-r", "realm1", "-c", "realm1,root,s3cr3t");
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Realm 'realm1' successfully bootstrapped.")
        .contains("Bootstrap completed successfully.");
  }
}
