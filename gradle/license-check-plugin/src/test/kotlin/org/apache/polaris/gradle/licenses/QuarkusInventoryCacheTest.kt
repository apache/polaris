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

import groovy.json.JsonSlurper
import java.io.File
import java.util.Properties
import java.util.jar.JarOutputStream
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class QuarkusInventoryCacheTest {
  @TempDir lateinit var dir: File

  @Test
  fun restoresInventoryWithCachedApplicationAndRejectsNewUnattributedDependency() {
    fixture()
    val inventory = File(dir, "build/quarkus-run-cyclonedx.json")
    fun run(extra: Boolean = false) = runner(extra).build()

    assertThat(run().task(":quarkusAppPartsBuild")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    val original = inventory.readText()
    assertThat(run().output).contains("Configuration cache entry reused")

    // A clean workspace must recover both the application and its native inventory.
    File(dir, "build").deleteRecursively()
    assertThat(run().task(":quarkusAppPartsBuild")?.outcome).isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(inventory.readText()).isEqualTo(original)

    // Missing only the side effect used to leave the producer UP-TO-DATE.
    inventory.delete()
    assertThat(run().task(":quarkusAppPartsBuild")?.outcome).isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(inventory.readText()).isEqualTo(original)

    // Keep attribution for the original dependency set, then cache a different application.
    val components = (JsonSlurper().parseText(original) as Map<*, *>)["components"] as List<*>
    File(dir, "LICENSE")
      .writeText(
        components
          .map { it as Map<*, *> }
          .filter { (it["purl"] as? String)?.startsWith("pkg:maven/") == true }
          .joinToString("\n", postfix = "\n") {
            "* Maven group:artifact IDs: ${it["group"]}:${it["name"]}"
          },
      )
    run(true)
    val expanded = inventory.readText()
    assertThat(expanded).contains("cache-fixture-extra")
    assertThat(original).doesNotContain("cache-fixture-extra")

    assertThat(run().task(":quarkusAppPartsBuild")?.outcome).isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(inventory.readText()).isEqualTo(original)
    val rejected = runner(true, "verifyLicenseAttribution").buildAndFail()
    assertThat(rejected.task(":quarkusAppPartsBuild")?.outcome).isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(inventory.readText()).isEqualTo(expanded)
    assertThat(rejected.task(":verifyLicenseAttribution")?.outcome)
      .describedAs(rejected.output)
      .isEqualTo(TaskOutcome.FAILED)
    assertThat(File(dir, "build/reports/licensee/attribution-validation.txt").readText())
      .contains("Shipped components missing LICENSE markers (1):\n- example:cache-fixture-extra")
  }

  private fun fixture() {
    File(dir, "settings.gradle")
      .writeText(
        """
        pluginManagement { repositories { mavenCentral(); gradlePluginPortal() } }
        rootProject.name = 'quarkus-cache-fixture'
        buildCache { local { directory = file('cache') } }
        """
          .trimIndent(),
      )
    val metadata = Properties()
    javaClass.classLoader.getResourceAsStream("plugin-under-test-metadata.properties").use {
      metadata.load(requireNotNull(it))
    }
    val classpath =
      metadata.getProperty("implementation-classpath").split(File.pathSeparator).joinToString(",") {
        "'" + it.replace("\\", "\\\\").replace("'", "\\'") + "'"
      }
    // Match Polaris's applied-script classpath isolation from the Quarkus plugin.
    File(dir, "licenses.gradle")
      .writeText(
        """
      buildscript { dependencies { classpath files($classpath) } }
      apply plugin: org.apache.polaris.gradle.licenses.LicenseChecksPlugin
      licenseChecks {
        policyFrom(layout.projectDirectory.file('policy.json'))
        licenseFile.set(layout.projectDirectory.file('LICENSE'))
        noticeFile.set(layout.projectDirectory.file('NOTICE'))
      }
      """
          .trimIndent(),
      )
    val quarkusVersion = System.getProperty("quarkusVersion")
    File(dir, "build.gradle")
      .writeText(
        """
      plugins {
        id 'java'
        id 'io.quarkus' version '$quarkusVersion'
      }
      group = 'example.app'
      version = '1'
      apply from: 'licenses.gradle'
      repositories { maven { url = uri('repo') }; mavenCentral() }
      dependencies {
        implementation enforcedPlatform('io.quarkus.platform:quarkus-bom:$quarkusVersion')
        implementation 'io.quarkus:quarkus-core'
        implementation 'io.quarkus:quarkus-cyclonedx'
        if (providers.gradleProperty('extra').getOrElse('false') == 'true') {
          runtimeOnly 'example:cache-fixture-extra:1'
        }
      }
      quarkus {
        quarkusBuildProperties.putAll([
          'quarkus.package.jar.type': 'fast-jar',
          'quarkus.cyclonedx.runtime-only': 'true',
          'quarkus.cyclonedx.schema-version': '1.6',
          'quarkus.cyclonedx.endpoint.enabled': 'false',
          'quarkus.cyclonedx.embedded.enabled': 'false'
        ])
      }
      """
          .trimIndent(),
      )
    File(dir, "NOTICE").writeText("Fixture notice\n")
    File(dir, "policy.json")
      .writeText(requireNotNull(javaClass.getResource("/license-policy.json")).readText())
    val repository = File(dir, "repo/example/cache-fixture-extra/1").apply { mkdirs() }
    File(repository, "cache-fixture-extra-1.pom")
      .writeText(
        """
        <project><modelVersion>4.0.0</modelVersion>
          <groupId>example</groupId><artifactId>cache-fixture-extra</artifactId><version>1</version>
          <licenses><license><name>Apache License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url></license></licenses>
        </project>
        """
          .trimIndent(),
      )
    JarOutputStream(File(repository, "cache-fixture-extra-1.jar").outputStream()).use {}
  }

  private fun runner(extra: Boolean, task: String = "quarkusBuild"): GradleRunner =
    GradleRunner.create()
      .withProjectDir(dir)
      .withArguments(
        task,
        "-Pextra=$extra",
        "--build-cache",
        "--configuration-cache",
        "--console=plain",
        "--stacktrace",
      )
}
