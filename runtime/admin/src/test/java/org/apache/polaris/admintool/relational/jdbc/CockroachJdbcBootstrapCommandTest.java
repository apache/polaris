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
import org.junit.jupiter.api.Test;

@TestProfile(CockroachJdbcAdminProfile.class)
public class CockroachJdbcBootstrapCommandTest extends RelationalJdbcBootstrapCommandTest {

  @Override
  @Test
  public void testBootstrapFailsWhenAddingRealmWithDifferentSchemaVersion(
      QuarkusMainLauncher launcher) {
    // CockroachDB only has schema v3 (no v1 or v2 schemas exist).
    // Override to bootstrap with v3, which is the only version available for CockroachDB.
    LaunchResult result1 =
        launcher.launch("bootstrap", "-v", "3", "-r", "realm1", "-c", "realm1,root,s3cr3t");
    assertThat(result1.exitCode()).isEqualTo(0);
    assertThat(result1.getOutput()).contains("Bootstrap completed successfully.");
  }
}
