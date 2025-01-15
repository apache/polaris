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
package org.apache.polaris.apprunner.plugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.BuildTask;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.TaskOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for the {@link PolarisRunnerPlugin}, which basically simulates what the {@code build.gradle}
 * in Apache Iceberg does.
 */
@ExtendWith(SoftAssertionsExtension.class)
class TestPolarisRunnerPlugin {
  @InjectSoftAssertions SoftAssertions soft;
  @TempDir Path testProjectDir;

  Path buildFile;

  String nessieVersionForTest;

  List<String> prefix;

  @BeforeEach
  void setup() throws Exception {
    buildFile = testProjectDir.resolve("build.gradle");
    var localBuildCacheDirectory = testProjectDir.resolve(".local-cache");

    // Copy our test class in the test's project test-source folder
    var testTargetDir = testProjectDir.resolve("src/test/java/org/apache/polaris/apprunner/plugin");
    Files.createDirectories(testTargetDir);
    Files.copy(
        Paths.get(
            "src/test/resources/org/apache/polaris/apprunner/plugin/TestSimulatingTestUsingThePlugin.java"),
        testTargetDir.resolve("TestSimulatingTestUsingThePlugin.java"));

    Files.write(
        testProjectDir.resolve("settings.gradle"),
        Arrays.asList(
            "buildCache {",
            "    local {",
            "        directory '" + localBuildCacheDirectory.toUri() + "'",
            "    }",
            "}",
            "",
            "include 'sub'"));

    // Versions injected from build.gradle.kts - this is actually a Nessie version
    nessieVersionForTest = System.getProperty("polaris-version-for-test", "0.101.3");
    var junitVersion = System.getProperty("junit-version");

    soft.assertThat(junitVersion != null)
        .withFailMessage(
            "System property required for this test is missing, run this test via Gradle or set the system properties manually")
        .isTrue();

    prefix =
        Arrays.asList(
            "plugins {",
            "    id 'java'",
            "    id 'org.apache.polaris.apprunner'",
            "}",
            "",
            "repositories {",
            "    mavenLocal()",
            "    mavenCentral()",
            "}",
            "",
            "test {",
            "    useJUnitPlatform()",
            "}",
            "",
            "dependencies {",
            "    testImplementation 'org.junit.jupiter:junit-jupiter-api:" + junitVersion + "'",
            "    testImplementation 'org.junit.jupiter:junit-jupiter-engine:" + junitVersion + "'",
            "    testImplementation 'org.projectnessie.nessie:nessie-client:"
                + nessieVersionForTest
                + "'");
  }

  /**
   * Ensure that the plugin fails when there is no dependency specified for the {@code
   * polarisQuarkusServer} configuration.
   */
  @Test
  void noAppConfigDeps() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "}", "", "polarisQuarkusApp {", "    includeTask(tasks.named(\"test\"))", "}"))
            .collect(Collectors.toList()));

    var result = createGradleRunner("test").buildAndFail();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    soft.assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains(
            "> Neither does the configuration polarisQuarkusServer contain exactly one dependency (preferably org.apache.polaris:polaris-quarkus-server:runner), nor is the runner jar specified in the polarisQuarkusApp extension.");
  }

  /**
   * Ensure that the plugin works with a declared dependency via the {@code polarisQuarkusServer}
   * configuration.
   */
  @Test
  void withDependencyDeclaration() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    polarisQuarkusServer 'org.projectnessie.nessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "}",
                    "",
                    "polarisQuarkusApp.includeTask(tasks.named(\"test\"))"))
            .collect(Collectors.toList()));

    var result = createGradleRunner("test").build();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.SUCCESS);
  }

  /**
   * Ensure that the plugin fails when there is more than one dependency specified for the {@code
   * polarisQuarkusServer} configuration.
   */
  @Test
  void tooManyAppConfigDeps() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    polarisQuarkusServer 'org.projectnessie.nessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "    polarisQuarkusServer 'org.projectnessie.nessie:nessie-model:"
                        + nessieVersionForTest
                        + "'",
                    "}",
                    "",
                    "polarisQuarkusApp.includeTask(tasks.named(\"test\"))"))
            .collect(Collectors.toList()));

    var result = createGradleRunner("test").buildAndFail();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    soft.assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains(
            "> Expected configuration polarisQuarkusServer to resolve to exactly one artifact, "
                + "but resolves to org.projectnessie.nessie:nessie-quarkus:"
                + nessieVersionForTest
                + ", org.projectnessie.nessie:nessie-model:"
                + nessieVersionForTest
                + " (hint: do not enable transitive on the dependency)");
  }

  /**
   * Ensure that the plugin fails when both the config-dependency and the exec-jar are specified.
   */
  @Test
  void configAndExecJar() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    polarisQuarkusServer 'org.projectnessie.nessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "}",
                    "",
                    "polarisQuarkusApp {",
                    "    executableJar.set(jar.archiveFile.get())",
                    "    includeTask(tasks.named(\"test\"))",
                    "}"))
            .collect(Collectors.toList()));

    soft.assertThat(createGradleRunner("jar").build().task(":jar"))
        .extracting(BuildTask::getOutcome)
        .isNotEqualTo(TaskOutcome.FAILED);

    var result = createGradleRunner("test").buildAndFail();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    soft.assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains(
            "> Configuration polarisQuarkusServer contains a dependency and option 'executableJar' are mutually exclusive");
  }

  /** Ensure that the plugin fails when it doesn't find a matching Java. */
  @Test
  void unknownJdk() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    polarisQuarkusServer 'org.projectnessie.nessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "}",
                    "",
                    "polarisQuarkusApp {",
                    "    javaVersion.set(42)",
                    "    includeTask(tasks.named(\"test\"))",
                    "}"))
            .collect(Collectors.toList()));

    BuildResult result = createGradleRunner("test").buildAndFail();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FAILED);
    soft.assertThat(Arrays.asList(result.getOutput().split("\n")))
        .contains("> " + ProcessState.noJavaMessage(42));
  }

  /**
   * Starting the Polaris-Server via the Polaris-Quarkus-Gradle-Plugin must work fine, even if a
   * different nessie-client version is being used (despite whether having conflicting versions
   * makes any sense).
   */
  @Test
  void conflictingDependenciesNessie() throws Exception {
    Files.write(
        buildFile,
        Stream.concat(
                prefix.stream(),
                Stream.of(
                    "    implementation 'org.projectnessie.nessie:nessie-client:0.101.0'",
                    "    polarisQuarkusServer 'org.projectnessie.nessie:nessie-quarkus:"
                        + nessieVersionForTest
                        + ":runner'",
                    "}",
                    "",
                    "polarisQuarkusApp {",
                    "    includeTask(tasks.named(\"test\"))",
                    "}"))
            .collect(Collectors.toList()));

    var result = createGradleRunner("test").build();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.SUCCESS);

    soft.assertThat(Arrays.asList(result.getOutput().split("\n")))
        .anyMatch(l -> l.contains("Listening on: http://0.0.0.0:"))
        .contains("Quarkus application stopped.");

    // 2nd run must be up-to-date

    result = createGradleRunner("test").build();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.UP_TO_DATE);

    // 3rd run after a 'clean' must use the cached result

    result = createGradleRunner("clean").build();
    soft.assertThat(result.task(":clean"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.SUCCESS);

    result = createGradleRunner("test").build();
    soft.assertThat(result.task(":test"))
        .isNotNull()
        .extracting(BuildTask::getOutcome)
        .isEqualTo(TaskOutcome.FROM_CACHE);
  }

  private GradleRunner createGradleRunner(String task) {
    return GradleRunner.create()
        .withPluginClasspath()
        .withProjectDir(testProjectDir.toFile())
        .withArguments("--no-configuration-cache", "--build-cache", "--info", "--stacktrace", task)
        .withDebug(true)
        .forwardOutput();
  }
}
