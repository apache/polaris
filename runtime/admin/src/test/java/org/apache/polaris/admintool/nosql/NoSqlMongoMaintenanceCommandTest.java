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
import org.junit.jupiter.api.Test;

@TestProfile(NoSqlMongoProfile.class)
@QuarkusMainTest
class NoSqlMongoMaintenanceCommandTest {
  @Test
  @Launch(value = {"help", "nosql"})
  void help(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Commands:")
        .contains("maintenance-log   Show Polaris persistence maintenance log.")
        .contains("maintenance-run   Run Polaris persistence maintenance.");
  }

  @Test
  @Launch(value = {"nosql"})
  void noArg(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Polaris NoSql persistence has multiple subcommands,")
        .contains("use the 'help nosql' command.")
        .contains("Information: selected NoSql persistence backend: MongoDb");
  }

  @Test
  @Launch(value = {"nosql", "maintenance-info"})
  void info(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Information: selected NoSql persistence backend: MongoDb")
        .contains("Maintenance configuration:");
  }

  @Test
  @Launch(value = {"nosql", "maintenance-log"})
  void logOfNothing(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Recorded Polaris NoSql persistence maintenance runs:")
        .contains("(none)");
  }

  @Test
  @Launch(value = {"nosql", "maintenance-run"})
  void run(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Maintenance configuration:")
        .contains("Process system realm: true")
        .contains("Starting NoSql persistence maintenance run...")
        .contains("Run started: ")
        .contains("status: (no exceptional information, all good so far)")
        .contains("finished: 20")
        .contains("References:")
        .contains("Objects:")
        .contains("Realm: ::system::");
  }
}
