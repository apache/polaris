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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import picocli.CommandLine;

@ExtendWith(MockitoExtension.class)
class BootstrapCommandTest {

  @Mock private MetaStoreManagerFactory factory;

  @Test
  void testIgnoreExistingSucceedsOnAlreadyBootstrappedRealm() {
    when(factory.bootstrapRealms(any(BootstrapOptions.class)))
        .thenReturn(
            Map.of(
                "realm1",
                new PrincipalSecretsResult(
                    BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, "realm already bootstrapped")));

    BootstrapCommand cmd = new BootstrapCommand();
    cmd.metaStoreManagerFactory = factory;
    CommandLine cli = new CommandLine(cmd);
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    cli.setOut(new PrintWriter(out, true));
    cli.setErr(new PrintWriter(err, true));

    int exitCode = cli.execute("-r", "realm1", "-c", "realm1,root,s3cr3t", "--ignore-existing");

    assertThat(exitCode).isEqualTo(0);
    assertThat(err.toString()).contains("realm1").contains("already bootstrapped");
    assertThat(out.toString()).contains("Bootstrap completed successfully.");
  }

  @Test
  void testAlreadyBootstrappedWithoutFlagFails() {
    when(factory.bootstrapRealms(any(BootstrapOptions.class)))
        .thenReturn(
            Map.of(
                "realm1",
                new PrincipalSecretsResult(
                    BaseResult.ReturnStatus.ENTITY_ALREADY_EXISTS, "realm already bootstrapped")));

    BootstrapCommand cmd = new BootstrapCommand();
    cmd.metaStoreManagerFactory = factory;
    CommandLine cli = new CommandLine(cmd);
    StringWriter err = new StringWriter();
    cli.setErr(new PrintWriter(err, true));

    int exitCode = cli.execute("-r", "realm1", "-c", "realm1,root,s3cr3t");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_BOOTSTRAP_ERROR);
    assertThat(err.toString()).contains("Bootstrap encountered errors during operation.");
  }

  @Test
  void testBootstrapInvalidCredentials() {
    CommandLine commandLine = new CommandLine(new BootstrapCommand());
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("-r", "realm1", "-c", "invalid syntax");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_BOOTSTRAP_ERROR);
    assertThat(err.toString())
        .contains("Invalid credentials format: invalid syntax")
        .contains("Bootstrap encountered errors during operation.");
  }

  @Test
  void testBootstrapInvalidArguments() {
    CommandLine commandLine = new CommandLine(new BootstrapCommand());
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("-r", "realm1", "-f", "/irrelevant");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_USAGE);
    assertThat(err.toString())
        .contains(
            "Error: [-r=<realm> [-r=<realm>]... [-c=<realm,clientId,clientSecret>]... [-p]] and [[-f=<file>]] are mutually exclusive (specify only one)");
  }

  @Test
  void testBootstrapFromInvalidFile() {
    CommandLine commandLine = new CommandLine(new BootstrapCommand());
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("-f", "/non/existing/file");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_BOOTSTRAP_ERROR);
    assertThat(err.toString())
        .contains("Failed to read credentials from file:///non/existing/file")
        .contains("Bootstrap encountered errors during operation.");
  }

  @Test
  void testNoPrintCredentialsSystemGenerated() {
    CommandLine commandLine = new CommandLine(new BootstrapCommand());
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("-r", "realm1");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_BOOTSTRAP_ERROR);
    assertThat(err.toString()).contains("--credentials").contains("--print-credentials");
  }

  @Test
  void testBootstrapInvalidArg() {
    CommandLine commandLine = new CommandLine(new BootstrapCommand());
    StringWriter err = new StringWriter();
    commandLine.setErr(new PrintWriter(err));

    int exitCode = commandLine.execute("-r", "realm1", "--not-real-arg");

    assertThat(exitCode).isEqualTo(BaseCommand.EXIT_CODE_USAGE);
    assertThat(err.toString()).contains("Unknown option: '--not-real-arg'").contains("Usage:");
  }
}
