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

package changelog

import java.io.File
import org.assertj.core.api.Assertions.assertThat
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Runs real Gradle builds on a fixture project that wires the changelog fragments tasks like the
 * root build does, together with the same `org.jetbrains.changelog` plugin.
 */
class ChangelogFragmentsGradleTest {

  @TempDir lateinit var project: File

  private val changelog
    get() = File(project, "CHANGELOG.md")

  @BeforeEach
  fun setUp() {
    val pluginVersion =
      Regex("""jetbrains-changelog\s*=\s*\{[^}]*version\s*=\s*"([^"]+)"""")
        .find(File(System.getProperty("libs.versions.toml")).readText())!!
        .groupValues[1]

    val buildLogicJar = File(System.getProperty("build-logic.jar")).invariantSeparatorsPath

    // The build cache directory is local to the fixture, to not share it with other builds
    File(project, "settings.gradle.kts")
      .writeText(
        """
        rootProject.name = "fixture"
        buildCache { local { directory = file("build-cache") } }
        """
          .trimIndent(),
      )
    // Keep the changelog configuration and the tasks in sync with the root build.gradle.kts
    File(project, "build.gradle.kts")
      .writeText(
        """
        import changelog.CheckChangelogFragmentsTask
        import changelog.MergeChangelogFragmentsTask

        buildscript { dependencies { classpath(files("$buildLogicJar")) } }

        plugins { id("org.jetbrains.changelog") version "$pluginVersion" }

        version = "1.1.0"

        changelog {
          repositoryUrl.set("https://github.com/apache/polaris")
          title.set("Apache Polaris Changelog")
          versionPrefix.set("apache-polaris-")
          header.set(provider { "${'$'}{version.get()}" })
          groups.set(listOf("Highlights", "Upgrade notes", "Breaking changes", "New Features",
            "Changes", "Deprecations", "Fixes", "Commits"))
          version.set(provider { project.version.toString() })
        }

        tasks.register<CheckChangelogFragmentsTask>("checkChangelogFragments") {
          fragmentsDirectory = layout.projectDirectory.dir("changelog.d")
          successMarker = layout.buildDirectory.file("changelog-fragments-check/success")
        }

        tasks.register<MergeChangelogFragmentsTask>("mergeChangelogFragments") {
          fragmentsDirectory = layout.projectDirectory.dir("changelog.d")
          changelogFile = layout.projectDirectory.file("CHANGELOG.md")
        }
        """
          .trimIndent(),
      )
    changelog.writeText(
      """
      # Apache Polaris Changelog

      ## [Unreleased]

      ### Highlights

      ### Upgrade notes

      ### Breaking changes

      ### New Features

      ### Changes

      ### Deprecations

      ### Fixes

      - Existing fix.

      ### Commits

      ## [1.0.0]

      ### Fixes

      - Old fix.
      """
        .trimIndent() + "\n",
    )
    File(project, "changelog.d").mkdirs()
  }

  private fun fragment(path: String, text: String) =
    File(project, "changelog.d/$path").also {
      it.parentFile.mkdirs()
      it.writeText(text)
    }

  private fun runner(buildCache: Boolean, vararg args: String) =
    GradleRunner.create()
      .withProjectDir(project)
      .withArguments(if (buildCache) "--build-cache" else "--no-build-cache", *args)

  private fun run(vararg args: String): BuildResult = runner(false, *args).build()

  private fun runWithBuildCache(vararg args: String): BuildResult = runner(true, *args).build()

  private fun section(heading: String): String {
    val text = changelog.readText()
    val start = text.indexOf(heading)
    check(start >= 0) { "$heading not found in:\n$text" }
    val end = text.indexOf("\n## ", start + heading.length)
    return if (end < 0) text.substring(start) else text.substring(start, end)
  }

  private fun fragmentFiles() =
    File(project, "changelog.d").walkTopDown().filter { it.isFile }.map { it.name }.toSet()

  @Test
  fun mergeThenPatchChangelogProducesTheReleaseSection() {
    fragment("fixes/99-new-fix.md", "New fix.\n")
    fragment("new-features/99-feature.md", "New feature.\n")

    run("mergeChangelogFragments")
    run("patchChangelog")

    assertThat(section("## [1.1.0]"))
      .contains("### New Features", "- New feature.", "### Fixes", "- Existing fix.", "- New fix.")
    assertThat(section("## [Unreleased]")).doesNotContain("New fix.", "Existing fix.")
    assertThat(section("## [1.0.0]")).contains("- Old fix.").doesNotContain("New fix.")
    assertThat(fragmentFiles()).isEmpty()
  }

  @Test
  fun laterReleaseCandidateFragmentsGoIntoTheExistingReleaseSection() {
    fragment("fixes/99-new-fix.md", "New fix.\n")
    run("mergeChangelogFragments")
    run("patchChangelog")

    fragment("fixes/99-rc1-fix.md", "RC1 fix.\n")
    fragment("deprecations/99-rc1-deprecation.md", "RC1 deprecation.\n")
    run("mergeChangelogFragments", "--release-version=1.1.0")

    assertThat(section("## [1.1.0]"))
      .contains("- Existing fix.", "- New fix.", "- RC1 fix.", "### Deprecations")
      .contains("- RC1 deprecation.")
    assertThat(section("## [Unreleased]")).doesNotContain("RC1")
    assertThat(fragmentFiles()).isEmpty()
  }

  @Test
  fun checkTaskIsUpToDateUntilFragmentsChangeAndFailsOnInvalidOnes() {
    fragment("fixes/99-new-fix.md", "New fix.\n")

    assertThat(run("checkChangelogFragments").task(":checkChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.SUCCESS)
    assertThat(run("checkChangelogFragments").task(":checkChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.UP_TO_DATE)

    fragment("fixes/99-other-fix.md", "Other fix.\n")
    assertThat(run("checkChangelogFragments").task(":checkChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.SUCCESS)

    fragment("fixes/Bad_Name.md", "- Invalid.\n")
    val failure = runner(false, "checkChangelogFragments").buildAndFail()
    assertThat(failure.task(":checkChangelogFragments")?.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(failure.output).contains("Invalid changelog fragments", "Bad_Name.md")
  }

  @Test
  fun checkTaskResultIsRestoredFromTheBuildCache() {
    fragment("fixes/99-new-fix.md", "New fix.\n")
    fragment("highlights/10-highlight.md", "Highlight.\n")
    val marker = File(project, "build/changelog-fragments-check/success")

    assertThat(
        runWithBuildCache("checkChangelogFragments").task(":checkChangelogFragments")?.outcome,
      )
      .isEqualTo(TaskOutcome.SUCCESS)
    // The marker lists the fragments in changelog order, with paths relative to changelog.d
    val expectedMarker =
      "Validated 2 changelog fragment(s):\nhighlights/10-highlight.md\nfixes/99-new-fix.md\n"
    assertThat(marker.readText()).isEqualTo(expectedMarker)

    File(project, "build").deleteRecursively()
    assertThat(
        runWithBuildCache("checkChangelogFragments").task(":checkChangelogFragments")?.outcome,
      )
      .isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(marker.readText()).isEqualTo(expectedMarker)

    // A different set of fragments is a different cache entry
    fragment("fixes/99-other-fix.md", "Other fix.\n")
    assertThat(
        runWithBuildCache("checkChangelogFragments").task(":checkChangelogFragments")?.outcome,
      )
      .isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun mergeTaskIsNeverCachedOrUpToDateAndRetryAddsNothing() {
    fragment("fixes/99-new-fix.md", "New fix.\n")

    assertThat(run("mergeChangelogFragments").task(":mergeChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.SUCCESS)
    val afterFirst = changelog.readText()
    assertThat(afterFirst).contains("- New fix.")

    val second = run("mergeChangelogFragments")
    assertThat(second.task(":mergeChangelogFragments")?.outcome).isEqualTo(TaskOutcome.SUCCESS)
    assertThat(second.output).contains("Merged 0 changelog fragment(s)")
    assertThat(changelog.readText()).isEqualTo(afterFirst)
  }
}
