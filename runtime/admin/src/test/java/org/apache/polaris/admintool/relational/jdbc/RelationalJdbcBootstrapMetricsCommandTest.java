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
import org.apache.polaris.admintool.BootstrapMetricsCommandTestBase;
import org.junit.jupiter.api.Test;

/**
 * JDBC-specific tests for {@link org.apache.polaris.admintool.BootstrapMetricsCommand}.
 *
 * <p>These tests verify the bootstrap-metrics command works correctly with the JDBC persistence
 * backend.
 *
 * <p><b>Note:</b> Tests that require state persistence across multiple {@code launcher.launch()}
 * calls are not possible with the current test framework because each launch gets a fresh
 * PostgreSQL database. See the TODO comment in {@link
 * RelationalJdbcBootstrapCommandTest#testBootstrapFailsWhenAddingRealmWithDifferentSchemaVersion}
 * for details.
 */
@TestProfile(RelationalJdbcAdminProfile.class)
public class RelationalJdbcBootstrapMetricsCommandTest extends BootstrapMetricsCommandTestBase {

  @Test
  public void testBootstrapMetricsForSingleRealm(QuarkusMainLauncher launcher) {
    // Bootstrap entity schema and metrics schema in one launch using --include-metrics.
    // Note: Each launcher.launch() gets a fresh database, so we use --include-metrics
    // to bootstrap both entity and metrics schema in a single launch.
    LaunchResult bootstrapResult =
        launcher.launch(
            "bootstrap",
            "-r",
            "metrics-realm1",
            "-c",
            "metrics-realm1,root,s3cr3t",
            "--include-metrics");
    assertThat(bootstrapResult.exitCode()).isEqualTo(0);
    assertThat(bootstrapResult.getOutput())
        .contains("Realm 'metrics-realm1' successfully bootstrapped.");
  }

  @Test
  public void testBootstrapMetricsMultipleRealmsInSingleLaunch(QuarkusMainLauncher launcher) {
    // Bootstrap entity schema and metrics schema for multiple realms in one launch
    LaunchResult result =
        launcher.launch(
            "bootstrap",
            "-r",
            "metrics-realm3",
            "-r",
            "metrics-realm4",
            "-c",
            "metrics-realm3,root,s3cr3t",
            "-c",
            "metrics-realm4,root,s3cr3t",
            "--include-metrics");
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Realm 'metrics-realm3' successfully bootstrapped.")
        .contains("Realm 'metrics-realm4' successfully bootstrapped.")
        .contains("Bootstrap completed successfully.");
  }

  // TODO: Enable these tests once we enable postgres container reuse across launches.
  // See
  // RelationalJdbcBootstrapCommandTest#testBootstrapFailsWhenAddingRealmWithDifferentSchemaVersion
  //
  // @Test
  // public void testBootstrapMetricsIdempotent(QuarkusMainLauncher launcher) {
  //   // First launch: bootstrap entity schema and metrics schema
  //   LaunchResult result1 = launcher.launch(
  //       "bootstrap", "-r", "realm1", "-c", "realm1,root,s3cr3t", "--include-metrics");
  //   assertThat(result1.exitCode()).isEqualTo(0);
  //
  //   // Second launch: bootstrap-metrics should detect it's already bootstrapped
  //   LaunchResult result2 = launcher.launch("bootstrap-metrics", "-r", "realm1");
  //   assertThat(result2.exitCode()).isEqualTo(0);
  //   assertThat(result2.getOutput())
  //       .contains("Metrics schema already bootstrapped for realm 'realm1'. Skipping.");
  // }
}
