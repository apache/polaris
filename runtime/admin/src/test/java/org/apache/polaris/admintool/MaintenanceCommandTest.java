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

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class MaintenanceCommandTest {

  @Test
  void testPurgeEventsSuccess() {
    MaintenanceCommand command = new MaintenanceCommand();
    command.maintenanceManager = () -> {};

    CommandLine commandLine = new CommandLine(command);
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    commandLine.setOut(new PrintWriter(out));
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("--purge-events");

    assertThat(exitCode).isEqualTo(0);
    assertThat(out.toString()).contains("Events purged successfully.");
  }

  @Test
  void testPurgeEventsUnsupportedBackend() {
    MaintenanceCommand command = new MaintenanceCommand();
    command.maintenanceManager = null;

    CommandLine commandLine = new CommandLine(command);
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    commandLine.setOut(new PrintWriter(out));
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("--purge-events");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_MAINTENANCE_ERROR);
    assertThat(err.toString())
        .contains("Event purge is not supported for the current persistence backend.");
  }

  @Test
  void testPurgeEventsFailurePrintsStackTrace() {
    String failureMessage = "simulated database connection failure";
    MaintenanceCommand command = new MaintenanceCommand();
    command.maintenanceManager =
        () -> {
          throw new RuntimeException(failureMessage);
        };

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("--purge-events");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_MAINTENANCE_ERROR);
    assertThat(err.toString())
        .contains("java.lang.RuntimeException: " + failureMessage)
        .contains("\tat ")
        .contains("Maintenance encountered errors during operation.");
  }

  @Test
  void testNoFlagPrintsUsage() {
    MaintenanceCommand command = new MaintenanceCommand();
    command.maintenanceManager = () -> {};

    CommandLine commandLine = new CommandLine(command);
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    commandLine.setOut(new PrintWriter(out));
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute();

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_USAGE);
    assertThat(err.toString()).contains("No maintenance operation specified.");
  }
}
