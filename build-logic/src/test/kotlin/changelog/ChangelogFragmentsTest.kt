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
import java.util.stream.Stream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.api.GradleException
import org.junit.jupiter.api.Assumptions.assumeFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

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

  /** Creates a fragment with a single list item for each of the given sections. */
  private fun fragment(name: String, vararg sections: Pair<String, String>) =
    rawFragment(
      name,
      sections.joinToString("\n") { (section, text) -> "### $section\n\n- $text\n" },
    )

  private fun fragment(name: String, section: String, text: String) =
    fragment(name, section to text)

  private fun rawFragment(name: String, content: String) =
    File(fragments, name).also { it.writeText(content) }

  private fun fragmentFiles() = fragments.walkTopDown().filter { it.isFile }.map { it.name }.toSet()

  @Test
  fun assemblyAppendsEachFragmentOnceToItsSection() {
    fragment("new-fix.md", "Fixes", "New fix.")
    // A fragment can have an entry in several sections
    fragment("new-stuff.md", "Highlights" to "New highlight.", "Upgrade notes" to "New note.")

    val consumed = ChangelogFragments.merge(fragments, changelog)

    assertThat(consumed.map { it.name }).containsExactlyInAnyOrder("new-fix.md", "new-stuff.md")
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
  fun aSectionCanHaveSeveralItemsAndTheyAreMergedAsWritten() {
    rawFragment(
      "several.md",
      """
      ### Fixes

      - First fix.

      - Second fix,
        wrapped.
      - Third fix.
      """
        .trimIndent() + "\n",
    )
    fragment("other.md", "Fixes", "Other fix.")

    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText())
      .contains(
        """
        ### Fixes

        - Existing fix.
        - Other fix.
        - First fix.

        - Second fix,
          wrapped.
        - Third fix.

        ### Commits
        """
          .trimIndent(),
      )
  }

  @Test
  fun markdownStructureIsPreserved() {
    rawFragment(
      "complex.md",
      """
      ### Changes

      - First line,
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
  fun codeInAnEntryIsNotMistakenForHeadings() {
    rawFragment(
      "with-code.md",
      """
      ### Fixes

      - Use the script:

        ```shell
        ### not a section
        # not a heading either
        run.sh
        ```
      """
        .trimIndent() + "\n",
    )

    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText())
      .contains(
        """
        - Existing fix.
        - Use the script:

          ```shell
          ### not a section
          # not a heading either
          run.sh
          ```
        """
          .trimIndent(),
      )
  }

  @Test
  fun entriesLandAfterTheLastItemOfASection() {
    changelog.writeText(
      """
      ## [Unreleased]

      ### Highlights
      - First item, right under the heading.

      - Second item,
        wrapped.

      - Last item.

      ### Upgrade notes

      ## [1.0.0]
      """
        .trimIndent() + "\n",
    )
    fragment("10-new.md", "Highlights", "New highlight.")
    fragment("99-newer.md", "Highlights", "Newer highlight.")

    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText())
      .isEqualTo(
        """
        ## [Unreleased]

        ### Highlights
        - First item, right under the heading.

        - Second item,
          wrapped.

        - Last item.
        - New highlight.
        - Newer highlight.

        ### Upgrade notes

        ## [1.0.0]
        """
          .trimIndent() + "\n",
      )
  }

  @Test
  fun entriesAreOrderedByFileNameSoANumberPrefixPrioritizesAnEntry() {
    fragment("b.md", "Fixes", "B.")
    fragment("10-z.md", "Fixes", "Z.")
    fragment("a.md", "Fixes", "A.")

    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText()).contains("- Existing fix.\n- Z.\n- A.\n- B.\n")
  }

  @Test
  fun consumedFragmentsAreDeletedAndRetryAddsNothing() {
    fragment("new-fix.md", "Fixes", "New fix.")

    ChangelogFragments.merge(fragments, changelog)
    val afterFirst = changelog.readText()

    assertThat(fragmentFiles()).containsExactly("README.md")
    assertThat(ChangelogFragments.merge(fragments, changelog)).isEmpty()
    assertThat(changelog.readText()).isEqualTo(afterFirst)
  }

  companion object {
    @JvmStatic
    fun invalidFragments(): Stream<Arguments> =
      Stream.of(
        arguments("Bad_Name.md", "### Fixes\n\n- Text\n", "file name must match"),
        arguments("notes.txt", "### Fixes\n\n- Text\n", "file name must match"),
        arguments("empty.md", "  \n", "file is empty"),
        arguments("no-heading.md", "- Just text\n", "text before the first section heading"),
        arguments("unknown.md", "### Bugs\n\n- Text\n", "unknown section 'Bugs'"),
        arguments("case.md", "### fixes\n\n- Text\n", "did you mean 'Fixes'?"),
        arguments(
          "duplicate.md",
          "### Fixes\n\n- A\n\n### Fixes\n\n- B\n",
          "section 'Fixes' appears more than once",
        ),
        arguments(
          "empty-section.md",
          "### Fixes\n\n### Changes\n\n- Text\n",
          "section 'Fixes' is empty",
        ),
        arguments("heading-only.md", "### Fixes\n", "section 'Fixes' is empty"),
        arguments(
          "no-marker.md",
          "### Fixes\n\nText\n",
          "no-marker.md:3: section 'Fixes' must start with a list item",
        ),
        arguments(
          "star-marker.md",
          "### Fixes\n\n* Text\n",
          "section 'Fixes' must start with a list item",
        ),
        arguments(
          "marker-without-space.md",
          "### Fixes\n\n-Text\n",
          "section 'Fixes' must start with a list item",
        ),
        arguments(
          "indented-start.md",
          "### Fixes\n\n  - Text\n",
          "section 'Fixes' must start with a list item",
        ),
        arguments(
          "unindented.md",
          "### Fixes\n\n- Text\nwrapped\n",
          "unindented.md:4: expected a list item starting with '- ' or a line indented by two spaces",
        ),
        arguments(
          "tab.md",
          "### Fixes\n\n- Text\n\twrapped\n",
          "expected a list item starting with '- ' or a line indented by two spaces",
        ),
        arguments(
          "h1.md",
          "### Fixes\n\n- Text\n\n# Fixes\n\n- More\n",
          "heading '# Fixes' is not allowed in an entry, write the section as '### Fixes'",
        ),
        arguments(
          "h2.md",
          "### Fixes\n\n- Text\n\n## Deprecation\n\n- More\n",
          "write the section as '### Deprecations'",
        ),
        arguments(
          "h4.md",
          "### Fixes\n\n- Text\n\n#### Details\n\n- More\n",
          "heading '#### Details' is not allowed in an entry, sections are written as '### <name>'",
        ),
        arguments(
          "heading-before-item.md",
          "### Fixes\n\n# Fixes\n\n- Text\n",
          "heading '# Fixes' is not allowed in an entry",
        ),
      )
  }

  @ParameterizedTest
  @MethodSource("invalidFragments")
  fun invalidFragmentsFailWithDiagnosticsAndChangeNothing(
    name: String,
    content: String,
    message: String,
  ) {
    fragment("valid.md", "Fixes", "Valid.")
    rawFragment(name, content)

    assertThatThrownBy { ChangelogFragments.merge(fragments, changelog) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining(name)
      .hasMessageContaining(message)

    assertThat(changelog.readText()).isEqualTo(template)
    assertThat(fragmentFiles()).contains("valid.md")
    assertThatThrownBy { ChangelogFragments.collect(fragments) }
      .isInstanceOf(GradleException::class.java)
  }

  @Test
  fun unknownSectionWithoutSimilarNameHasNoSuggestion() {
    fragment("noteworthy.md", "New & Noteworthy", "Text.")

    assertThatThrownBy { ChangelogFragments.collect(fragments) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("unknown section 'New & Noteworthy', must be one of")
      .hasMessageNotContaining("did you mean")
  }

  @Test
  fun missingHeadingFailsBeforeWritingAnything() {
    fragment("feature.md", "New Features", "Feature.")
    fragment("fix.md", "Fixes", "Fix.")

    assertThatThrownBy { ChangelogFragments.merge(fragments, changelog) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("### New Features")

    assertThat(changelog.readText()).isEqualTo(template)
    assertThat(fragmentFiles()).contains("feature.md", "fix.md")
  }

  @Test
  fun undeletableFragmentsFailBeforeWritingAnything() {
    val f = fragment("fix.md", "Fixes", "Fix.")
    Files.setPosixFilePermissions(fragments.toPath(), PosixFilePermissions.fromString("r-xr-xr-x"))
    try {
      // Skip when running as a user that ignores directory permissions, e.g. root
      assumeFalse(Files.isWritable(fragments.toPath()))

      assertThatThrownBy { ChangelogFragments.merge(fragments, changelog) }
        .isInstanceOf(GradleException::class.java)
        .hasMessageContaining("nothing was changed")

      assertThat(changelog.readText()).isEqualTo(template)
      assertThat(f).exists()
    } finally {
      Files.setPosixFilePermissions(
        fragments.toPath(),
        PosixFilePermissions.fromString("rwxr-xr-x"),
      )
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
    fragment("fix.md", "Fixes", "New fix.")
    fragment("highlight.md", "Highlights", "New highlight.")
    fragment("feature.md", "New Features", "New feature.")
    fragment("dep.md", "Deprecations", "Deprecation.")

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
    fragment("fix.md", "Fixes", "New fix.")

    ChangelogFragments.merge(fragments, changelog, "1.1.0")

    assertThat(changelog.readText())
      .contains("- Existing note.\n\n### Fixes\n\n- New fix.\n\n## [1.0.0]")
  }

  @Test
  fun unknownReleaseVersionFailsAndChangesNothing() {
    fragment("fix.md", "Fixes", "Fix.")

    assertThatThrownBy { ChangelogFragments.merge(fragments, changelog, "9.9.9") }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("## [9.9.9]")

    assertThat(changelog.readText()).isEqualTo(template)
    assertThat(fragmentFiles()).contains("fix.md")
  }

  @Test
  fun orderingDoesNotDependOnTheOrderOfTheDirectoryListing() {
    val files = listOf("b.md", "10-z.md", "a.md", "05-m.md").map { File(it) }

    val expected = listOf("05-m.md", "10-z.md", "a.md", "b.md")
    assertThat(ChangelogFragments.orderFragments(files).map { it.name }).isEqualTo(expected)
    assertThat(ChangelogFragments.orderFragments(files.reversed()).map { it.name })
      .isEqualTo(expected)
  }

  @Test
  fun failedDeletionAfterWritingIsReportedWithRecoveryInstructions() {
    fragment("10-a.md", "Fixes", "A.")
    val b = fragment("20-b.md", "Fixes", "B.")
    fragment("30-c.md", "Fixes", "C.")

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
  fun directoryIsReportedAsNotAFile() {
    File(fragments, "99-dir.md").mkdirs()
    // The layout before fragments were flat: one directory per section
    File(fragments, "fixes").mkdirs()

    assertThatThrownBy { ChangelogFragments.collect(fragments) }
      .isInstanceOf(GradleException::class.java)
      .hasMessageContaining("99-dir.md: must be a file, not a directory")
      .hasMessageContaining("fixes: must be a file, not a directory")
      .hasMessageNotContaining("file name must match")
  }

  @Test
  fun hiddenFilesAreIgnored() {
    File(fragments, ".DS_Store").writeText("junk")
    fragment("fix.md", "Fixes", "Fix.")

    assertThat(ChangelogFragments.collect(fragments).map { it.file.name }).containsExactly("fix.md")
    ChangelogFragments.merge(fragments, changelog)

    assertThat(changelog.readText()).contains("- Existing fix.\n- Fix.\n")
  }
}
