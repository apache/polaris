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
 * root build does.
 */
class ChangelogFragmentsGradleTest {

  @TempDir lateinit var project: File

  private val changelog
    get() = File(project, "CHANGELOG.md")

  @BeforeEach
  fun setUp() {
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
    // Keep the tasks in sync with the root build.gradle.kts
    File(project, "build.gradle.kts")
      .writeText(
        """
        import changelog.CheckChangelogFragmentsTask
        import changelog.MergeChangelogFragmentsTask
        import changelog.ReleaseChangelogTask

        buildscript { dependencies { classpath(files("$buildLogicJar")) } }

        tasks.register<CheckChangelogFragmentsTask>("checkChangelogFragments") {
          fragmentsDirectory = layout.projectDirectory.dir("changelog.d")
          successMarker = layout.buildDirectory.file("changelog-fragments-check/success")
        }

        tasks.register<ReleaseChangelogTask>("releaseChangelog") {
          fragmentsDirectory = layout.projectDirectory.dir("changelog.d")
          changelogFile = layout.projectDirectory.file("CHANGELOG.md")
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

      [Unreleased]: https://github.com/apache/polaris/compare/apache-polaris-1.0.0...HEAD
      [1.0.0]: https://github.com/apache/polaris/compare/apache-polaris-0.9.0...apache-polaris-1.0.0
      """
        .trimIndent() + "\n",
    )
    File(project, "changelog.d").mkdirs()
  }

  /** Creates a fragment with a single list item for each of the given sections. */
  private fun fragment(name: String, vararg sections: Pair<String, String>) =
    File(project, "changelog.d/$name").also {
      it.writeText(sections.joinToString("\n") { (section, text) -> "### $section\n\n- $text\n" })
    }

  private fun runner(buildCache: Boolean, vararg args: String) =
    GradleRunner.create()
      .withProjectDir(project)
      // The tests change files between builds. Under load, the file system watcher of the daemon
      // can lag behind and miss the change, so the task wrongly is up to date.
      .withArguments(
        if (buildCache) "--build-cache" else "--no-build-cache",
        "--no-watch-fs",
        *args,
      )

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
  fun releaseChangelogProducesTheReleaseSection() {
    fragment("new-fix.md", "Fixes" to "New fix.")
    fragment("feature.md", "New Features" to "New feature.", "Upgrade notes" to "New note.")

    run("releaseChangelog", "--release-version=1.1.0")

    assertThat(section("## [1.1.0]"))
      .contains("### New Features", "- New feature.")
      .contains("### Fixes", "- Existing fix.", "- New fix.")
      .contains("### Upgrade notes", "- New note.")
    assertThat(section("## [Unreleased]")).doesNotContain("New fix.", "Existing fix.")
    assertThat(section("## [1.0.0]")).contains("- Old fix.").doesNotContain("New fix.")
    assertThat(changelog.readText())
      .contains(
        "[Unreleased]: https://github.com/apache/polaris/compare/apache-polaris-1.1.0...HEAD\n" +
          "[1.1.0]: https://github.com/apache/polaris/compare/apache-polaris-1.0.0...apache-polaris-1.1.0\n",
      )
    assertThat(fragmentFiles()).isEmpty()
  }

  @Test
  fun laterReleaseCandidateFragmentsGoIntoTheExistingReleaseSection() {
    fragment("new-fix.md", "Fixes" to "New fix.")
    run("releaseChangelog", "--release-version=1.1.0")

    fragment("rc1.md", "Fixes" to "RC1 fix.", "Deprecations" to "RC1 deprecation.")
    run("mergeChangelogFragments", "--release-version=1.1.0")

    assertThat(section("## [1.1.0]"))
      .contains("- Existing fix.", "- New fix.", "- RC1 fix.", "### Deprecations")
      .contains("- RC1 deprecation.")
    assertThat(section("## [Unreleased]")).doesNotContain("RC1")
    assertThat(fragmentFiles()).isEmpty()
  }

  @Test
  fun releaseChangelogRequiresAVersionThatDoesNotExistYetAndIsNeverUpToDate() {
    fragment("new-fix.md", "Fixes" to "New fix.")

    val withoutVersion = runner(false, "releaseChangelog").buildAndFail()
    assertThat(withoutVersion.output).contains("The release version option is required")
    assertThat(changelog.readText()).doesNotContain("## [1.1.0]")

    assertThat(
        run("releaseChangelog", "--release-version=1.1.0").task(":releaseChangelog")?.outcome,
      )
      .isEqualTo(TaskOutcome.SUCCESS)
    val afterRelease = changelog.readText()

    // Running it again is not up to date, and the release exists now: it fails and changes nothing
    fragment("other-fix.md", "Fixes" to "Other fix.")
    val again = runner(false, "releaseChangelog", "--release-version=1.1.0").buildAndFail()
    assertThat(again.task(":releaseChangelog")?.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(again.output).contains("Section '## [1.1.0]' already exists")
    assertThat(changelog.readText()).isEqualTo(afterRelease)
    assertThat(fragmentFiles()).containsExactly("other-fix.md")
  }

  @Test
  fun checkTaskIsUpToDateUntilFragmentsChangeAndFailsOnInvalidOnes() {
    fragment("new-fix.md", "Fixes" to "New fix.")

    assertThat(run("checkChangelogFragments").task(":checkChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.SUCCESS)
    assertThat(run("checkChangelogFragments").task(":checkChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.UP_TO_DATE)

    fragment("other-fix.md", "Fixes" to "Other fix.")
    assertThat(run("checkChangelogFragments").task(":checkChangelogFragments")?.outcome)
      .isEqualTo(TaskOutcome.SUCCESS)

    fragment("Bad_Name.md", "Fixes" to "Invalid.")
    val failure = runner(false, "checkChangelogFragments").buildAndFail()
    assertThat(failure.task(":checkChangelogFragments")?.outcome).isEqualTo(TaskOutcome.FAILED)
    assertThat(failure.output).contains("Invalid changelog fragments", "Bad_Name.md")
  }

  @Test
  fun checkTaskResultIsRestoredFromTheBuildCache() {
    fragment("new-fix.md", "Fixes" to "New fix.")
    fragment("highlight.md", "Highlights" to "Highlight.", "Upgrade notes" to "Note.")
    val marker = File(project, "build/changelog-fragments-check/success")

    assertThat(
        runWithBuildCache("checkChangelogFragments").task(":checkChangelogFragments")?.outcome,
      )
      .isEqualTo(TaskOutcome.SUCCESS)
    // The marker lists the fragments in order of their file names, with their sections
    val expectedMarker =
      "Validated 2 changelog fragment(s):\n" +
        "highlight.md: Highlights, Upgrade notes\n" +
        "new-fix.md: Fixes\n"
    assertThat(marker.readText()).isEqualTo(expectedMarker)

    File(project, "build").deleteRecursively()
    assertThat(
        runWithBuildCache("checkChangelogFragments").task(":checkChangelogFragments")?.outcome,
      )
      .isEqualTo(TaskOutcome.FROM_CACHE)
    assertThat(marker.readText()).isEqualTo(expectedMarker)

    // A different set of fragments is a different cache entry
    fragment("other-fix.md", "Fixes" to "Other fix.")
    assertThat(
        runWithBuildCache("checkChangelogFragments").task(":checkChangelogFragments")?.outcome,
      )
      .isEqualTo(TaskOutcome.SUCCESS)
  }

  @Test
  fun mergeTaskIsNeverCachedOrUpToDateAndRetryAddsNothing() {
    fragment("new-fix.md", "Fixes" to "New fix.")

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
