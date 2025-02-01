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

import static org.apache.polaris.admintool.BaseCommand.EXIT_CODE_BOOTSTRAP_ERROR;
import static org.apache.polaris.admintool.BaseCommand.EXIT_CODE_USAGE;
import static org.apache.polaris.admintool.PostgresTestResourceLifecycleManager.INIT_SCRIPT;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.common.TestResourceScope;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
@WithTestResource(
    value = PostgresTestResourceLifecycleManager.class,
    scope = TestResourceScope.GLOBAL,
    initArgs = @ResourceArg(name = INIT_SCRIPT, value = "org/apache/polaris/admintool/init.sql"))
class BootstrapCommandTest {

  private static Path json;
  private static Path yaml;

  @BeforeAll
  static void prepareFiles() throws IOException {
    json = copyResource("/org/apache/polaris/admintool/credentials.json");
    yaml = copyResource("/org/apache/polaris/admintool/credentials.yaml");
  }

  @Test
  @Launch(
      value = {
        "bootstrap",
        "-r",
        "realm1",
        "-r",
        "realm2",
        "-c",
        "realm1,root,s3cr3t",
        "-c",
        "realm2,root,s3cr3t"
      })
  public void testBootstrapFromCommandLineArguments(LaunchResult result) {
    assertThat(result.getOutput())
        .contains("Realm 'realm1' successfully bootstrapped.")
        .contains("Realm 'realm2' successfully bootstrapped.")
        .contains("Bootstrap completed successfully.");
  }

  @Test
  @Launch(
      value = {
        "bootstrap",
        "-r",
        "realm1",
        "-c",
        "invalid syntax",
      },
      exitCode = EXIT_CODE_BOOTSTRAP_ERROR)
  public void testBootstrapInvalidCredentials(LaunchResult result) {
    assertThat(result.getErrorOutput())
        .contains("Invalid credentials format: invalid syntax")
        .contains("Bootstrap encountered errors during operation.");
  }

  @Test
  @Launch(
      value = {"bootstrap", "-r", "realm1", "-f", "/irrelevant"},
      exitCode = EXIT_CODE_USAGE)
  public void testBootstrapInvalidArguments(LaunchResult result) {
    assertThat(result.getErrorOutput())
        .contains(
            "Error: (-r=<realm> [-r=<realm>]... [-c=<realm,clientId,clientSecret>]...) "
                + "and -f=<file> are mutually exclusive (specify only one)");
  }

  @Test
  public void testBootstrapFromValidJsonFile(QuarkusMainLauncher launcher) {
    LaunchResult result = launcher.launch("bootstrap", "-f", json.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Realm 'realm1' successfully bootstrapped.")
        .contains("Realm 'realm2' successfully bootstrapped.")
        .contains("Bootstrap completed successfully.");
  }

  @Test
  public void testBootstrapFromValidYamlFile(QuarkusMainLauncher launcher) {
    LaunchResult result = launcher.launch("bootstrap", "-f", yaml.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Realm 'realm1' successfully bootstrapped.")
        .contains("Realm 'realm2' successfully bootstrapped.")
        .contains("Bootstrap completed successfully.");
  }

  @Test
  public void testBootstrapFromInvalidFile(QuarkusMainLauncher launcher) {
    LaunchResult result = launcher.launch("bootstrap", "-f", "/non/existing/file");
    assertThat(result.exitCode()).isEqualTo(EXIT_CODE_BOOTSTRAP_ERROR);
    assertThat(result.getErrorOutput())
        .contains("Failed to read credentials file: file:/non/existing/file")
        .contains("Bootstrap encountered errors during operation.");
  }

  private static Path copyResource(String resource) throws IOException {
    URL jsonSource = Objects.requireNonNull(BootstrapCommandTest.class.getResource(resource));
    Path file = Files.createTempFile("credentials", "tmp");
    file.toFile().deleteOnExit();
    try (InputStream is = jsonSource.openStream()) {
      Files.copy(is, file, StandardCopyOption.REPLACE_EXISTING);
    }
    return file;
  }
}
