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
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteExisting
import kotlin.io.path.readText
import kotlin.io.path.writeText
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class DistributionLicenseChecksPluginTest {
  @TempDir lateinit var directory: Path

  @Test
  fun verifiesBothFormatsAndReusesConfigurationCache() {
    fixture()
    val first = runner().build()
    for (kind in listOf("Zip", "Tar")) {
      assertThat(first.task(":verifyDist${kind}Attribution")?.outcome)
        .isEqualTo(TaskOutcome.SUCCESS)
    }
    assertThat(first.output).contains("Configuration cache entry stored")
    val second = runner().build()
    assertThat(second.output).contains("Configuration cache entry reused")
    for (kind in listOf("Zip", "Tar")) {
      assertThat(second.task(":verifyDist${kind}Attribution")?.outcome)
        .isEqualTo(TaskOutcome.UP_TO_DATE)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = ["Zip", "Tar"])
  fun directArchiveInvocationRejectsChangedDeliveredNoticeWithReusedCache(kind: String) {
    fixture()
    runner("dist$kind").build()
    directory.resolve("delivered/NOTICE").writeText("Changed delivered notice\n")
    val failed = runner("dist$kind").buildAndFail()
    assertThat(failed.output).contains("Configuration cache entry reused")
    assertThat(failed.task(":verifyDist${kind}Attribution")?.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(report(kind)).contains("Delivered NOTICE differs from attribution input")
  }

  @ParameterizedTest
  @ValueSource(strings = ["LICENSE", "NOTICE"])
  fun rejectsMissingDeliveredResource(name: String) {
    fixture()
    directory.resolve("delivered/$name").deleteExisting()
    runner("distZip").buildAndFail()
    assertThat(report("Zip")).contains("Expected exactly one distribution $name, found 0")
  }

  @Test
  fun changedAttributionInputIsRecheckedWithReusedCache() {
    fixture()
    runner("distZip").build()
    directory.resolve("attribution/LICENSE").writeText("Changed source license\n")
    val failed = runner("distZip").buildAndFail()
    assertThat(failed.output).contains("Configuration cache entry reused")
    assertThat(report("Zip")).contains("Delivered LICENSE differs from attribution input")
  }

  @Test
  fun rejectsBlankNoticeEvenWhenDeliveredBytesMatch() {
    fixture()
    directory.resolve("attribution/NOTICE").writeText(" \n")
    directory.resolve("delivered/NOTICE").writeText(" \n")
    runner("distTar").buildAndFail()
    assertThat(report("Tar")).contains("NOTICE input is blank")
  }

  private fun fixture() {
    directory
      .resolve("settings.gradle.kts")
      .writeText("rootProject.name = \"distribution-fixture\"")
    directory
      .resolve("build.gradle.kts")
      .writeText(
        """
        plugins {
          distribution
          id("polaris-distribution-license-checks")
        }
        tasks.register<Sync>("licenseNoticeMerge") {
          from("attribution")
          into(layout.buildDirectory.dir("merged-license-notice"))
        }
        distributions { main { contents { from("delivered") } } }
        tasks.named<Tar>("distTar") { compression = Compression.GZIP }
        """
          .trimIndent(),
      )
    for (folder in listOf("attribution", "delivered")) {
      directory.resolve(folder).createDirectories()
      directory.resolve("$folder/LICENSE").writeText("Fixture license\n")
      directory.resolve("$folder/NOTICE").writeText("Fixture notice\n")
    }
  }

  private fun report(kind: String) =
    directory.resolve("build/reports/licensee/dist-${kind.lowercase()}-attribution.txt").readText()

  private fun runner(task: String = "checkDistributionLicenses"): GradleRunner =
    GradleRunner.create()
      .withProjectDir(directory.toFile())
      .withPluginClasspath()
      .withArguments(task, "--configuration-cache", "--console=plain")
}
