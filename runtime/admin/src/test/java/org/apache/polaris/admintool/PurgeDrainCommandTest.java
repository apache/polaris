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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import picocli.CommandLine;

/**
 * Covers the option and backend checks the command makes before it reaches the drainer; the drain
 * itself is covered against PostgreSQL and CockroachDB by {@link PurgeDrainCommandTestBase}.
 */
class PurgeDrainCommandTest {

  @ParameterizedTest
  @CsvSource({"--batch-size,0", "--batch-size,-1", "--claim-lease-ms,0", "--claim-lease-ms,-5"})
  void rejectsNonPositiveOptionsWithoutDraining(String option, String value) {
    // The drainer is left unset: a rejected option must be refused before it is reached.
    PurgeDrainCommand command = new PurgeDrainCommand();
    command.persistenceConfiguration = () -> "relational-jdbc";

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    assertThat(commandLine.execute(option, value)).isNotZero();
    assertThat(err.toString()).contains(option + " must be strictly positive");
  }

  @Test
  void reportsAUsageErrorOnABackendWithoutAPurgeQueue() {
    PurgeDrainCommand command = new PurgeDrainCommand();
    command.persistenceConfiguration = () -> "nosql";

    CommandLine commandLine = new CommandLine(command);
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    assertThat(commandLine.execute()).isEqualTo(BaseCommand.EXIT_CODE_USAGE);
    assertThat(err.toString()).contains("purge-drain applies to the 'relational-jdbc'");
  }
}
