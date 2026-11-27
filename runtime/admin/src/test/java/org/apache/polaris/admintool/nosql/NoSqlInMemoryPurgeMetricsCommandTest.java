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

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.apache.polaris.admintool.AdminProfiles;
import org.apache.polaris.admintool.BaseCommand;
import org.junit.jupiter.api.Test;

@TestProfile(AdminProfiles.NoSqlInMemory.class)
@QuarkusMainTest
class NoSqlInMemoryPurgeMetricsCommandTest {
  @Test
  @Launch(
      value = {"purge-metrics", "-r", "realm1", "--older-than", "P1D"},
      exitCode = BaseCommand.EXIT_CODE_USAGE)
  void unsupported(LaunchResult result) {
    assertThat(result.getErrorOutput())
        .contains("Purging metrics reports is only supported with the relational-jdbc persistence");
  }
}
