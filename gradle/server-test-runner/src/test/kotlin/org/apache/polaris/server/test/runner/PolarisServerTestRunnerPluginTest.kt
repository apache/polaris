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
package org.apache.polaris.server.test.runner

import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.writeText
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class PolarisServerTestRunnerPluginTest {
  @TempDir lateinit var projectDir: Path

  @Test
  fun `starts server for test task and passes discovered ports`() {
    writeSettings()
    writeBuild(
      """
      import org.gradle.api.tasks.compile.JavaCompile
      import org.gradle.jvm.tasks.Jar

      plugins {
        java
        id("polaris-server-test-runner")
      }

      repositories { mavenCentral() }

      dependencies {
        testImplementation(platform("org.junit:junit-bom:6.1.0"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
      }

      val serverRuntime by configurations.creating

      dependencies {
        serverRuntime(files(tasks.named("jar")))
      }

      val startupActionSourceDir = layout.buildDirectory.dir("startup-action-src")
      val startupActionClassesDir = layout.buildDirectory.dir("startup-action-classes")

      val writeStartupAction by tasks.registering {
        val sourceFile = startupActionSourceDir.map { it.file("test/StartupAction.java") }
        outputs.file(sourceFile)
        doLast {
          sourceFile.get().asFile.apply {
            parentFile.mkdirs()
            writeText(
              ${"\"\"\""}
              package test;

              import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction;
              import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext;

              public class StartupAction implements PolarisServerStartupAction {
                @Override
                public void start(PolarisServerStartupContext context) {
                  context.getSystemProperties().put(
                      "startup.value", context.getParameters().get("value"));
                }
              }
              ${"\"\"\""}.trimIndent()
            )
          }
        }
      }

      val compileStartupAction by tasks.registering(JavaCompile::class) {
        dependsOn(writeStartupAction)
        source(startupActionSourceDir)
        destinationDirectory.set(startupActionClassesDir)
        classpath =
          files(
            org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction::class.java
              .protectionDomain
              .codeSource
              .location
          )
      }

      val startupActionJar by tasks.registering(Jar::class) {
        dependsOn(compileStartupAction)
        from(startupActionClassesDir)
        archiveBaseName.set("startup-action")
      }

      tasks.named<Jar>("jar") {
        manifest { attributes("Main-Class" to "test.FakeServer") }
      }

      tasks.test {
        useJUnitPlatform()
        withPolarisServer(configurations.named("serverRuntime")) {
          arguments.add(layout.buildDirectory.file("server-stopped.txt").get().asFile.absolutePath)
          arguments.add(layout.buildDirectory.file("server-started.txt").get().asFile.absolutePath)
          startupActionClasspath.from(startupActionJar)
          startupActionClass.set("test.StartupAction")
          startupActionParameters.put("value", "started")
        }
      }
      """
        .trimIndent()
    )
    writeFakeServer()
    writePropertyTest()

    val result = gradleRunner("test").build()

    assertThat(result.task(":test")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(projectDir.resolve("build/server-stopped.txt")).exists()
    assertThat(projectDir.resolve("build/server-started.txt")).hasContent("started")
  }

  @Test
  fun `fails when server artifact is missing`() {
    writeSettings()
    writeBuild(
      """
      plugins {
        java
        id("polaris-server-test-runner")
      }

      repositories { mavenCentral() }

      dependencies {
        testImplementation(platform("org.junit:junit-bom:6.1.0"))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
      }

      tasks.test {
        useJUnitPlatform()
        withPolarisServer(files())
      }
      """
        .trimIndent()
    )
    writePropertyTest()

    val result = gradleRunner("test").buildAndFail()

    assertThat(result.output).contains("Expected exactly one Polaris server artifact")
  }

  private fun writeSettings() {
    projectDir.resolve("settings.gradle.kts").writeText("rootProject.name = \"fixture\"\n")
  }

  private fun writeBuild(content: String) {
    projectDir.resolve("build.gradle.kts").writeText(content)
  }

  private fun writeFakeServer() {
    val sourceDir = projectDir.resolve("src/main/java/test").createDirectories()
    sourceDir
      .resolve("FakeServer.java")
      .writeText(
        """
        package test;

        import java.nio.file.Files;
        import java.nio.file.Path;
        import java.util.concurrent.CountDownLatch;

        public final class FakeServer {
          public static void main(String[] args) throws Exception {
            Path stopped = Path.of(args[0]);
            if (args.length > 1) {
              String startupValue = System.getProperty("startup.value");
              if (!"started".equals(startupValue)) {
                throw new IllegalStateException("Unexpected startup value: " + startupValue);
              }
              Files.writeString(Path.of(args[1]), startupValue);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
              try {
                Files.writeString(stopped, "stopped");
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }));
            System.out.println("Listening on: http://0.0.0.0:12345. Management interface listening on http://0.0.0.0:12346.");
            new CountDownLatch(1).await();
          }
        }
        """
          .trimIndent()
      )
  }

  private fun writePropertyTest() {
    val testDir = projectDir.resolve("src/test/java/test").createDirectories()
    testDir
      .resolve("ServerPropertiesTest.java")
      .writeText(
        """
        package test;

        import static org.junit.jupiter.api.Assertions.assertEquals;

        import org.junit.jupiter.api.Test;

        public class ServerPropertiesTest {
          @Test
          void receivesServerProperties() {
            assertEquals("12345", System.getProperty("quarkus.http.test-port"));
            assertEquals("12346", System.getProperty("quarkus.management.test-port"));
          }
        }
        """
          .trimIndent()
      )
  }

  private fun gradleRunner(vararg arguments: String): GradleRunner =
    GradleRunner.create()
      .withProjectDir(projectDir.toFile())
      .withPluginClasspath()
      .withArguments(*arguments, "--stacktrace")
      .forwardOutput()
}
