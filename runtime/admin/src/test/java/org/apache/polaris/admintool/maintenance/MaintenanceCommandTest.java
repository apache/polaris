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
package org.apache.polaris.admintool.maintenance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import org.apache.polaris.admintool.BaseCommand;
import org.apache.polaris.core.persistence.MaintenanceManager;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class MaintenanceCommandTest {

  @Test
  void testPurgeEventsSuccess() {
    PurgeEventsCommand command = new PurgeEventsCommand();
    command.maintenanceManager = cutoff -> {};

    CommandLine commandLine = new CommandLine(command);
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out));
    commandLine.setErr(new PrintWriter(new StringWriter()));

    int exitCode = commandLine.execute();

    assertThat(exitCode).isEqualTo(0);
    assertThat(out.toString()).contains("All events purged successfully.");
  }

  @Test
  void testPurgeEventsWithRetention() {
    PurgeEventsCommand command = new PurgeEventsCommand();
    Instant[] capturedCutoff = new Instant[1];
    command.maintenanceManager = cutoff -> capturedCutoff[0] = cutoff;

    CommandLine commandLine = new CommandLine(command);
    StringWriter out = new StringWriter();
    commandLine.setOut(new PrintWriter(out));
    commandLine.setErr(new PrintWriter(new StringWriter()));

    int exitCode = commandLine.execute("--retention-days", "7");

    assertThat(exitCode).isEqualTo(0);
    assertThat(out.toString()).contains("Events older than 7 days purged successfully.");
    assertThat(capturedCutoff[0]).isNotNull();
    assertThat(capturedCutoff[0]).isBefore(Instant.now());
  }

  @Test
  void testPurgeEventsUnsupportedBackend() {
    PurgeEventsCommand command = new PurgeEventsCommand();
    command.maintenanceManager = null;

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setOut(new PrintWriter(new StringWriter()));
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute();

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_MAINTENANCE_ERROR);
    assertThat(err.toString())
        .contains("Event purge is not supported for the current persistence backend.");
  }

  @Test
  void testPurgeEventsFailurePrintsStackTrace() {
    String failureMessage = "simulated database connection failure";
    PurgeEventsCommand command = new PurgeEventsCommand();
    command.maintenanceManager =
        (MaintenanceManager)
            cutoff -> {
              throw new RuntimeException(failureMessage);
            };

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute();

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_MAINTENANCE_ERROR);
    assertThat(err.toString())
        .contains("java.lang.RuntimeException: " + failureMessage)
        .contains("\tat ")
        .contains("Maintenance encountered errors during operation.");
  }
}
