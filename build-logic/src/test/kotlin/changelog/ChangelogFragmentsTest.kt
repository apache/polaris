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
import java.io.IOException
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.api.GradleException
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class ChangelogFragmentsTest {

  @TempDir lateinit var dir: File

  private lateinit var fragments: File
  private lateinit var changelog: File

  private val template =
    """
    # Changelog

    ## [Unreleased]

    ### Highlights

    ### Upgrade notes

    - Existing note.

    [Link]: https://example.com

    ### Fixes

    - Existing fix.

    ### Commits

    ## [1.0.0]

    ### Fixes

    - Old fix.
    """
      .trimIndent() + "\n"

  @BeforeEach
  fun setUp() {
    fragments = File(dir, "changelog.d").also { it.mkdirs() }
    File(fragments, "README.md").writeText("readme")
    changelog = File(dir, "CHANGELOG.md").also { it.writeText(template) }
  }

  private fun fragment(path: String, text: String) =
    File(fragments, path).also {
      it.parentFile.mkdirs()
      it.writeText(text)
    }

  private fun fragmentFiles() = fragments.walkTopDown().filter { it.isFile }.map { it.name }.toSet()

  @Test
  fun assemblyAppendsEachFragmentOnceToItsSection() {
    fragment("fixes/99-new-fix.md", "New fix.\n")
    fragment("highlights/99-new-highlight.md", "New highlight.\n")
    fragment("upgrade-notes/99-new-note.md", "New note.\n")

    val consumed = ChangelogFragments.merge(fragments, changelog)

    assertThat(consumed).hasSize(3)
    assertThat(changelog.readText())
      .isEqualTo(
        """
        # Changelog

        ## [Unreleased]

        ### Highlights

        - New highlight.

        ### Upgrade notes

        - Existing note.
        - New note.

        [Link]: https://example.com

        ### Fixes

        - Existing fix.
        - New fix.

        ### Commits

        ## [1.0.0]

        ### Fixes

        - Old fix.
        """
          .trimIndent() + "\n",
      )
  }

  @Test
  fun markdownStructureIsPreserved() {
    fragment(
      "changes/99-complex.md",
      """
      First line,
      wrapped.

      Second paragraph.

      - nested
        - deeper
      """
        .trimIndent() + "\n",
    )
    val withChanges =
      template.replace("### Fixes\n\n- Existing", "### Changes\n\n### Fixes\n\n- Existing")
    changelog.writeText(withChanges)

    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText())
      .contains(
        """
        ### Changes

        - First line,
          wrapped.

          Second paragraph.

          - nested
            - deeper

        ### Fixes
        """
          .trimIndent(),
      )
  }

  @Test
  fun entriesAreOrderedByPriorityThenName() {
    fragment("fixes/99-b.md", "B.\n")
    fragment("fixes/10-z.md", "Z.\n")
    fragment("fixes/99-a.md", "A.\n")

    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText()).contains("- Existing fix.\n- Z.\n- A.\n- B.\n")
  }

  @Test
  fun consumedFragmentsAreDeletedAndRetryAddsNothing() {
    fragment("fixes/99-new-fix.md", "New fix.\n")

    ChangelogFragments.merge(fragments, changelog)
    val afterFirst = changelog.readText()

    assertThat(fragmentFiles()).containsExactly("README.md")
    assertThat(File(fragments, "fixes")).doesNotExist()
    assertThat(ChangelogFragments.merge(fragments, changelog)).isEmpty()
    assertThat(changelog.readText()).isEqualTo(afterFirst)
  }

  @ParameterizedTest
  @CsvSource(
    "bogus/99-x.md,           unknown section",
    "fixes/Bad_Name.md,       file name must match",
    "fixes/9-short.md,        file name must match",
    "fixes/99-empty.md,       file is empty",
    "fixes/99-list.md,        must not start with a list marker",
  )
  fun invalidFragmentsFailWithDiagnosticsAndChangeNothing(path: String, message: String) {
    fragment("fixes/99-valid.md", "Valid.\n")
    val text = if (path.endsWith("empty.md")) "  \n" else "- bullet\n"
    fragment(path, text)

    assertThatThrownBy { ChangelogFragments.merge(fragments, changelog) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining(message)

    assertThat(changelog.readText()).isEqualTo(template)
    assertThat(fragmentFiles()).contains("99-valid.md")
    assertThatThrownBy { ChangelogFragments.collect(fragments) }
      .isInstanceOf(GradleException::class.java)
  }

  @Test
  fun missingHeadingFailsBeforeWritingAnything() {
    fragment("new-features/99-feature.md", "Feature.\n")
    fragment("fixes/99-fix.md", "Fix.\n")

    assertThatThrownBy { ChangelogFragments.merge(fragments, changelog) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("### New Features")

    assertThat(changelog.readText()).isEqualTo(template)
    assertThat(fragmentFiles()).contains("99-feature.md", "99-fix.md")
  }

  @Test
  fun undeletableFragmentsFailBeforeWritingAnything() {
    val f = fragment("fixes/99-fix.md", "Fix.\n")
    val section = f.parentFile
    Files.setPosixFilePermissions(section.toPath(), PosixFilePermissions.fromString("r-xr-xr-x"))
    try {
      // Skip when running as a user that ignores directory permissions, e.g. root
      assumeFalse(Files.isWritable(section.toPath()))

      assertThatThrownBy { ChangelogFragments.merge(fragments, changelog) }
        .isInstanceOf(GradleException::class.java)
        .hasMessageContaining("nothing was changed")

      assertThat(changelog.readText()).isEqualTo(template)
      assertThat(f).exists()
    } finally {
      Files.setPosixFilePermissions(section.toPath(), PosixFilePermissions.fromString("rwxr-xr-x"))
    }
  }

  private val released =
    """
    # Changelog

    ## [Unreleased]

    ### Fixes

    ## [1.1.0]

    ### Upgrade notes

    - Existing note.

    ### Fixes

    - Existing fix.

    ## [1.0.0]

    ### Fixes

    - Old fix.

    [1.1.0]: https://example.com/1.1.0
    """
      .trimIndent() + "\n"

  @Test
  fun releaseVersionMergesIntoExistingAndNewSubsectionsOfThatSection() {
    changelog.writeText(released)
    fragment("fixes/99-fix.md", "New fix.\n")
    fragment("highlights/99-highlight.md", "New highlight.\n")
    fragment("new-features/99-feature.md", "New feature.\n")
    fragment("deprecations/99-dep.md", "Deprecation.\n")

    ChangelogFragments.merge(fragments, changelog, "1.1.0")

    assertThat(changelog.readText())
      .isEqualTo(
        """
        # Changelog

        ## [Unreleased]

        ### Fixes

        ## [1.1.0]

        ### Highlights

        - New highlight.

        ### Upgrade notes

        - Existing note.

        ### New Features

        - New feature.

        ### Deprecations

        - Deprecation.

        ### Fixes

        - Existing fix.
        - New fix.

        ## [1.0.0]

        ### Fixes

        - Old fix.

        [1.1.0]: https://example.com/1.1.0
        """
          .trimIndent() + "\n",
      )
    assertThat(fragmentFiles()).containsExactly("README.md")
  }

  @Test
  fun releaseVersionCreatesMissingSubsectionAtTheEndOfTheSection() {
    changelog.writeText(released.replace("### Fixes\n\n- Existing fix.\n\n", ""))
    fragment("fixes/99-fix.md", "New fix.\n")

    ChangelogFragments.merge(fragments, changelog, "1.1.0")

    assertThat(changelog.readText())
      .contains("- Existing note.\n\n### Fixes\n\n- New fix.\n\n## [1.0.0]")
  }

  @Test
  fun unknownReleaseVersionFailsAndChangesNothing() {
    fragment("fixes/99-fix.md", "Fix.\n")

    assertThatThrownBy { ChangelogFragments.merge(fragments, changelog, "9.9.9") }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("## [9.9.9]")

    assertThat(changelog.readText()).isEqualTo(template)
    assertThat(fragmentFiles()).contains("99-fix.md")
  }

  @Test
  fun orderingDoesNotDependOnTheOrderOfTheDirectoryListing() {
    val files = listOf("99-b.md", "10-z.md", "99-a.md", "05-m.md").map { File(it) }

    val expected = listOf("05-m.md", "10-z.md", "99-a.md", "99-b.md")
    assertThat(ChangelogFragments.orderFragments(files).map { it.name }).isEqualTo(expected)
    assertThat(ChangelogFragments.orderFragments(files.reversed()).map { it.name })
      .isEqualTo(expected)
  }

  @Test
  fun failedDeletionAfterWritingIsReportedWithRecoveryInstructions() {
    fragment("fixes/10-a.md", "A.\n")
    val b = fragment("fixes/20-b.md", "B.\n")
    fragment("fixes/30-c.md", "C.\n")

    assertThatThrownBy {
        ChangelogFragments.merge(fragments, changelog) { f ->
          if (f == b) throw IOException("denied") else Files.delete(f.toPath())
        }
      }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("was updated, but these fragments could not be deleted")
      .hasMessageContaining(b.path)
      .hasMessageContaining("Do NOT run the merge again")
      .hasMessageNotContaining("10-a.md")
      .hasMessageNotContaining("30-c.md")

    // The changelog has all entries, once. Only the fragment that could not be deleted remains.
    assertThat(changelog.readText()).contains("- A.\n- B.\n- C.\n")
    assertThat(fragmentFiles()).containsExactlyInAnyOrder("README.md", "20-b.md")
  }

  @Test
  fun directoryInsideSectionIsReportedAsNotAFile() {
    File(fragments, "fixes/99-dir.md").mkdirs()

    assertThatThrownBy { ChangelogFragments.collect(fragments) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("99-dir.md: must be a file, not a directory")
      .hasMessageNotContaining("file name must match")
  }
}
