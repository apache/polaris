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
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import org.gradle.api.GradleException

/**
 * A changelog fragment: the list items, in Markdown, of each section of the file, in file order.
 * The sections are headings of the changelog, like `Fixes`.
 */
class Fragment(val file: File, val sections: Map<String, String>)

/**
 * Validates changelog fragments and merges them into the changelog file, see
 * `changelog.d/README.md`.
 */
object ChangelogFragments {

  /**
   * The sections of a fragment, in changelog order. They are headings of the Unreleased section.
   */
  val SECTIONS =
    listOf(
      "Highlights",
      "Upgrade notes",
      "Breaking changes",
      "New Features",
      "Changes",
      "Deprecations",
      "Fixes",
    )

  private val FRAGMENT_NAME = Regex("[a-z0-9][a-z0-9-]*\\.md")
  private val HEADING = Regex("^#{1,6}(\\s|$)")
  private val LINK_REFERENCE = Regex("\\[[^]]+]:.*")

  /** Lists the files of a directory, ignoring hidden files like `.DS_Store`, which Git ignores. */
  private fun visibleFiles(directory: File): List<File> =
    (directory.listFiles() ?: emptyArray()).filterNot { it.name.startsWith(".") }

  /**
   * Orders the fragments by file name, which can start with a number to prioritize an entry.
   * Directory listings have no defined order.
   */
  internal fun orderFragments(files: List<File>): List<File> = files.sortedBy { it.name }

  /**
   * Returns the fragments in the directory, ordered by file name. Fails if any fragment is invalid.
   */
  fun collect(directory: File): List<Fragment> {
    val errors = mutableListOf<String>()
    val fragments =
      orderFragments(visibleFiles(directory))
        .filter { it.name != "README.md" }
        .mapNotNull { f ->
          when {
            !f.isFile -> {
              errors.add("$f: must be a file, not a directory")
              null
            }
            !FRAGMENT_NAME.matches(f.name) -> {
              errors.add("$f: file name must match short-title.md, for example my-change.md")
              null
            }
            else -> Fragment(f, parse(f, errors))
          }
        }
    if (errors.isNotEmpty()) {
      throw GradleException("Invalid changelog fragments:\n" + errors.joinToString("\n"))
    }
    return fragments
  }

  /** Returns the section that [heading] is most likely a typo of, if any. */
  private fun closestSection(heading: String): String? = SECTIONS.firstOrNull { section ->
    section.equals(heading, ignoreCase = true) ||
      (heading.length >= 3 &&
        (section.startsWith(heading, ignoreCase = true) ||
          heading.startsWith(section, ignoreCase = true)))
  }

  /**
   * Splits the file into sections, each starting with a heading like `### Fixes`, followed by list
   * items like in the changelog. Each line of a section is blank, starts a list item with `- `, or
   * continues an item and is indented by two spaces. Therefore, a line starting with `#` is always
   * a heading, even if the entry has some code that contains such lines.
   */
  private fun parse(file: File, errors: MutableList<String>): Map<String, String> {
    val errorsBefore = errors.size
    val sections = linkedMapOf<String, String>()
    val seen = mutableSetOf<String>()
    var current: String? = null
    var ignoring = false
    var leadingTextReported = false
    var itemStarted = false
    var startReported = false
    val body = mutableListOf<String>()

    fun finishSection() {
      val section = current ?: return
      val text = body.joinToString("\n").trim()
      if (!startReported) {
        if (text.isEmpty()) errors.add("$file: section '$section' is empty")
        else sections[section] = text
      }
      body.clear()
      itemStarted = false
      startReported = false
    }

    file.readLines().forEachIndexed { index, line ->
      val where = "$file:${index + 1}"
      when {
        line.startsWith("### ") -> {
          finishSection()
          val heading = line.removePrefix("### ").trim()
          current = null
          ignoring = true
          when {
            heading !in SECTIONS ->
              errors.add(
                "$where: unknown section '$heading', must be one of $SECTIONS" +
                  (closestSection(heading)?.let { " (did you mean '$it'?)" } ?: ""),
              )
            !seen.add(heading) -> errors.add("$where: section '$heading' appears more than once")
            else -> {
              current = heading
              ignoring = false
            }
          }
        }
        current == null -> {
          if (!ignoring && line.isNotBlank() && !leadingTextReported) {
            leadingTextReported = true
            errors.add("$where: text before the first section heading, start with e.g. '### Fixes'")
          }
        }
        line.isBlank() -> body.add("")
        line.startsWith("- ") -> {
          itemStarted = true
          body.add(line)
        }
        HEADING.containsMatchIn(line) -> {
          val suggestion =
            closestSection(line.trimStart('#').trim())?.let { "write the section as '### $it'" }
              ?: "sections are written as '### <name>'"
          errors.add("$where: heading '${line.trim()}' is not allowed in an entry, $suggestion")
        }
        !itemStarted -> {
          if (!startReported) {
            startReported = true
            errors.add("$where: section '$current' must start with a list item, like '- Fixed ...'")
          }
        }
        line.startsWith("  ") -> body.add(line)
        else ->
          errors.add(
            "$where: expected a list item starting with '- ' or a line indented by two spaces",
          )
      }
    }
    finishSection()

    if (sections.isEmpty() && errors.size == errorsBefore) {
      errors.add("$file: file is empty, it needs a section like '### Fixes' with a list item")
    }
    return sections
  }

  /**
   * Appends the fragments to the matching sections of the Unreleased section of the changelog file,
   * or of the section of the given released [version], then deletes them. Returns the consumed
   * fragment files. Targeting a released version is for fragments cherry-picked onto a release
   * branch after the changelog section of that version was created, i.e. for later release
   * candidates.
   *
   * Invalid fragments and missing headings are reported before the changelog is written. Deletion
   * is also pre-checked for obvious problems (for example, when a fragment directory is not
   * writable).
   *
   * The changelog file itself is replaced atomically. Fragment deletion happens after the changelog
   * update, so unexpected deletion failures can still occur after writing; in that case, the
   * changelog may already be updated and the remaining fragments must be deleted manually (do not
   * run the merge again, or their entries would be added twice).
   */
  fun merge(
    directory: File,
    changelog: File,
    version: String? = null,
    delete: (File) -> Unit = { Files.delete(it.toPath()) },
  ): List<File> {
    val fragments = collect(directory)
    val consumed = fragments.map { it.file }
    val lines = changelog.readLines()
    val merged = appendFragments(lines, fragments, version)

    val undeletable = consumed.filter { !Files.isWritable(it.parentFile.toPath()) }
    if (undeletable.isNotEmpty()) {
      throw GradleException(
        "Cannot delete changelog fragments, nothing was changed:\n" +
          undeletable.joinToString("\n"),
      )
    }

    val temp = Files.createTempFile(changelog.parentFile.toPath(), changelog.name, ".tmp")
    try {
      Files.writeString(temp, merged.joinToString("\n", postfix = "\n"))
      Files.move(
        temp,
        changelog.toPath(),
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE,
      )
    } finally {
      Files.deleteIfExists(temp)
    }

    val remaining = consumed.filter { f -> runCatching { delete(f) }.isFailure }
    if (remaining.isNotEmpty()) {
      throw GradleException(
        "${changelog.name} was updated, but these fragments could not be deleted:\n" +
          remaining.joinToString("\n") +
          "\nDelete them manually. Do NOT run the merge again, it would add their entries twice.",
      )
    }
    return consumed
  }

  internal fun appendFragments(
    changelog: List<String>,
    fragments: List<Fragment>,
    version: String? = null,
  ): List<String> {
    val lines = changelog.toMutableList()
    val target = "## [${version ?: "Unreleased"}]"
    for (section in SECTIONS) {
      val texts = fragments.mapNotNull { it.sections[section] }
      if (texts.isEmpty()) continue
      val unreleased = lines.indexOf(target)
      if (unreleased < 0) throw GradleException("Section '$target' not found")
      var unreleasedEnd =
        (unreleased + 1 until lines.size).firstOrNull { lines[it].startsWith("## ") } ?: lines.size
      var heading = (unreleased + 1 until unreleasedEnd).firstOrNull { lines[it] == "### $section" }
      if (heading == null) {
        // A released section only has the subsections that had entries, Unreleased has them all
        if (version == null) {
          throw GradleException("Section '### $section' not found under '$target'")
        }
        heading = createSubsection(lines, unreleased + 1, unreleasedEnd, section)
        unreleasedEnd =
          (heading + 1 until lines.size).firstOrNull { lines[it].startsWith("## ") } ?: lines.size
      }
      val sectionEnd =
        (heading + 1 until unreleasedEnd).firstOrNull { lines[it].startsWith("### ") }
          ?: unreleasedEnd
      // Append the entries after the last entry of the section, before link reference definitions
      var insertAt = sectionEnd
      while (lines[insertAt - 1].isBlank() || LINK_REFERENCE.matches(lines[insertAt - 1])) {
        insertAt--
      }
      val entries = texts.flatMap { it.lines() }
      lines.addAll(
        insertAt,
        buildList {
          addAll(if (insertAt == heading + 1) listOf("") else listOf())
          addAll(entries)
          addAll(if (insertAt == sectionEnd) listOf("") else listOf())
        },
      )
    }
    return lines
  }

  /**
   * Inserts an empty `### [section]` heading in its place among the existing subsections of the
   * section spanning [start] until [end], and returns the index of the heading.
   */
  private fun createSubsection(
    lines: MutableList<String>,
    start: Int,
    end: Int,
    section: String,
  ): Int {
    val order = SECTIONS + "Commits"
    val following =
      (start until end).firstOrNull { i ->
        lines[i].startsWith("### ") &&
          order.indexOf(lines[i].removePrefix("### ")) > order.indexOf(section)
      }
    if (following != null) {
      lines.addAll(following, listOf("### $section", ""))
      return following
    }
    var insertAt = end
    while (
      insertAt > start &&
        (lines[insertAt - 1].isBlank() || LINK_REFERENCE.matches(lines[insertAt - 1]))
    ) {
      insertAt--
    }
    lines.addAll(insertAt, listOf("", "### $section"))
    return insertAt + 1
  }
}
