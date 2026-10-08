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

package org.apache.polaris.gradle.licenses

import java.nio.file.Path
import java.util.jar.JarOutputStream
import kotlin.io.path.createDirectories
import kotlin.io.path.outputStream
import kotlin.io.path.readText
import kotlin.io.path.writeText
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class LicenseChecksPluginTest {
  @TempDir lateinit var projectDir: Path

  @Test
  fun shadowConfigurationUsesItsInventoryProducerWithoutQuarkusTasks() {
    fixture()
    val build = projectDir.resolve("build.gradle.kts")
    build.writeText(
      build
        .readText()
        .replace(
          "tasks.register(\"quarkusAppPartsBuild\")",
          "tasks.register(\"generateLicenseInventory\")",
        ) +
        """

        licenseChecks {
          dependencyConfiguration.set("runtimeClasspath")
          inventoryTask.set("generateLicenseInventory")
          generatedComponentNames.set(emptySet())
        }
        """
          .trimIndent(),
    )
    val result = runner().build()
    assertThat(result.task(":verifyLicenseAttribution")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(result.task(":generateLicenseInventory")?.outcome).isEqualTo(TaskOutcome.UP_TO_DATE)
    assertThat(result.task(":quarkusAppPartsBuild")).isNull()
    assertThat(runner().build().output).contains("Configuration cache entry reused")
  }

  @Test
  fun policyChangesInvalidateCacheAndModuleAllowancesRemainEffective() {
    fixture()
    val first = runner().build()
    assertThat(first.task(":verifyLicenseAttribution")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(first.output).contains("Configuration cache entry stored")
    assertThat(runner().build().output).contains("Configuration cache entry reused")

    projectDir.resolve("policy.json").writeText("""{"allowedLicenses": []}""")
    val rejected = runner().buildAndFail()
    assertThat(rejected.output).contains("policy.json", "has changed", "example:apache:1.0")
    assertThat(rejected.task(":licensee")?.outcome).isEqualTo(TaskOutcome.FAILED)
    // MIT remains allowed by the module's existing Licensee DSL, independent of the policy file.
    assertThat(rejected.output).doesNotContain("example:mit:1.0")

    projectDir.resolve("policy.json").writeText("""{"allowedLicenses": ["Apache-2.0"]}""")
    assertThat(runner().build().task(":verifyLicenseAttribution")?.outcome)
      .isIn(TaskOutcome.SUCCESS, TaskOutcome.FROM_CACHE, TaskOutcome.UP_TO_DATE)
  }

  @Test
  fun missingAttributionFailsWhenConfigurationCacheIsReused() {
    fixture()
    runner().build()
    projectDir.resolve("LICENSE").writeText("* Maven group:artifact IDs: example:mit\n")
    val rejected = runner().buildAndFail()
    assertThat(rejected.output).contains("Configuration cache entry reused")
    assertThat(rejected.task(":verifyLicenseAttribution")?.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(projectDir.resolve("build/reports/licensee/attribution-validation.txt").readText())
      .contains("Shipped components missing LICENSE markers (1):\n- example:apache")
  }

  @ParameterizedTest
  @ValueSource(strings = ["NOTICE", "sbom.json"])
  fun changedVerificationInputFailsWhenConfigurationCacheIsReused(input: String) {
    fixture()
    runner().build()
    projectDir.resolve(input).writeText(if (input == "NOTICE") " \n" else """{"components": []}""")
    val rejected = runner().buildAndFail()
    assertThat(rejected.output).contains("Configuration cache entry reused")
    assertThat(rejected.task(":verifyLicenseAttribution")?.outcome).isEqualTo(TaskOutcome.FAILED)
  }

  @Test
  fun commonGateGuardsCheckAndMavenPublishingWithoutRunningPublication() {
    fixture()
    projectDir
      .resolve("build.gradle.kts")
      .toFile()
      .appendText(
        """

        publishing {
          publications.create<MavenPublication>("fixture") { from(components["java"]) }
          repositories { maven { url = uri("unused-publication-repo") } }
        }
        tasks.register("assertLicenseGates") {
          doLast {
            val gate = tasks.named("checkDistributionLicenses").get()
            check(tasks.named("check").get().taskDependencies.getDependencies(null).contains(gate))
            val publishTasks = tasks.withType<AbstractPublishToMaven>().toList()
            check(publishTasks.size == 2)
            publishTasks.forEach { check(it.taskDependencies.getDependencies(it).contains(gate)) }
            check(gate.taskDependencies.getDependencies(gate).contains(tasks.named("verifyLicenseAttribution").get()))
          }
        }
        """
          .trimIndent(),
      )
    val result =
      runner()
        .withArguments("assertLicenseGates", "--no-configuration-cache", "--console=plain")
        .build()
    assertThat(result.task(":assertLicenseGates")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(result.tasks.map { it.path })
      .doesNotContain(":licensee", ":publish", ":publishToMavenLocal")
  }

  @ParameterizedTest
  @ValueSource(
    strings =
      [
        """{"allowedLicense": []}""",
        """{"allowedUrls": [{"url": "https://example.test/license"}]}""",
      ],
  )
  fun invalidPolicyFailsDuringConfiguration(policy: String) {
    fixture()
    projectDir.resolve("policy.json").writeText(policy)
    assertThat(runner().buildAndFail().output)
      .containsAnyOf("Unknown license policy field", "allowedUrls entries must contain exactly")
  }

  private fun fixture() {
    projectDir
      .resolve("settings.gradle.kts")
      .writeText("""rootProject.name = "license-check-fixture"""")
    projectDir
      .resolve("build.gradle.kts")
      .writeText(
        """
        import org.gradle.api.publish.maven.tasks.AbstractPublishToMaven

        plugins {
          java
          `maven-publish`
          id("polaris-license-checks")
        }
        repositories { maven { url = uri("repo") } }
        configurations.create("quarkusProdRuntimeClasspathConfiguration") {
          extendsFrom(configurations.runtimeClasspath.get())
        }
        dependencies {
          runtimeOnly("example:apache:1.0")
          runtimeOnly("example:mit:1.0")
        }
        tasks.register("quarkusAppPartsBuild")
        licenseChecks {
          policyFrom(layout.projectDirectory.file("policy.json"))
          sbom.set(layout.projectDirectory.file("sbom.json"))
          licenseFile.set(layout.projectDirectory.file("LICENSE"))
          noticeFile.set(layout.projectDirectory.file("NOTICE"))
        }
        licensee { allow("MIT") }
        """
          .trimIndent(),
      )
    projectDir.resolve("policy.json").writeText("""{"allowedLicenses": ["Apache-2.0"]}""")
    projectDir
      .resolve("LICENSE")
      .writeText(
        "* Maven group:artifact IDs: example:apache\n* Maven group:artifact IDs: example:mit\n",
      )
    projectDir.resolve("NOTICE").writeText("Fixture notice\n")
    projectDir
      .resolve("sbom.json")
      .writeText(
        """{"components": [
        {"group": "example", "name": "apache", "version": "1.0", "purl": "pkg:maven/example/apache@1.0"},
        {"group": "example", "name": "mit", "version": "1.0", "purl": "pkg:maven/example/mit@1.0"}
      ]}""",
      )
    artifact(
      "apache",
      "Apache License, Version 2.0",
      "https://www.apache.org/licenses/LICENSE-2.0.txt",
    )
    artifact("mit", "MIT License", "https://opensource.org/licenses/MIT")
  }

  private fun artifact(name: String, license: String, url: String) {
    val directory = projectDir.resolve("repo/example/$name/1.0").createDirectories()
    directory
      .resolve("$name-1.0.pom")
      .writeText(
        """
      <project>
        <modelVersion>4.0.0</modelVersion>
        <groupId>example</groupId><artifactId>$name</artifactId><version>1.0</version>
        <licenses><license><name>$license</name><url>$url</url></license></licenses>
      </project>
      """
          .trimIndent(),
      )
    JarOutputStream(directory.resolve("$name-1.0.jar").outputStream()).use {}
  }

  private fun runner(): GradleRunner =
    GradleRunner.create()
      .withProjectDir(projectDir.toFile())
      .withPluginClasspath()
      .withArguments("verifyLicenseAttribution", "--configuration-cache", "--console=plain")
}
