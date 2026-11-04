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
 * Validates changelog fragments and merges them into the changelog file, see
 * `changelog.d/README.md`.
 */
object ChangelogFragments {

  /** Maps each section directory to the matching heading of the Unreleased section. */
  val SECTIONS =
    mapOf(
      "highlights" to "Highlights",
      "upgrade-notes" to "Upgrade notes",
      "breaking-changes" to "Breaking changes",
      "new-features" to "New Features",
      "changes" to "Changes",
      "deprecations" to "Deprecations",
      "fixes" to "Fixes",
    )

  private val FRAGMENT_NAME = Regex("[0-9]{2}-[a-z0-9][a-z0-9-]*\\.md")
  private val LIST_MARKER = Regex("^[-*+] ")
  private val LINK_REFERENCE = Regex("\\[[^]]+]:.*")

  /**
   * Returns the fragment files per section heading, in changelog order, sorted by file name within
   * a section. Fails if any fragment is invalid.
   */
  fun collect(directory: File): Map<String, List<File>> {
    val errors = mutableListOf<String>()
    val fragments = mutableMapOf<String, List<File>>()
    (directory.listFiles() ?: emptyArray())
      .sortedBy { it.name }
      .forEach { f ->
        val section = SECTIONS[f.name]
        if (f.isDirectory && section != null) {
          fragments[section] = f.listFiles()!!.sortedBy { it.name }
        } else if (f.name != "README.md") {
          errors.add("$f: unknown section, must be one of ${SECTIONS.keys}")
        }
      }
    fragments.values.flatten().forEach { f ->
      val text = if (f.isFile) f.readText().trim() else ""
      when {
        !f.isFile || !FRAGMENT_NAME.matches(f.name) ->
          errors.add("$f: file name must match NN-short-title.md, for example 99-my-change.md")
        text.isEmpty() -> errors.add("$f: file is empty")
        LIST_MARKER.containsMatchIn(text) ->
          errors.add("$f: must not start with a list marker, it is added when merging")
      }
    }
    if (errors.isNotEmpty()) {
      throw GradleException("Invalid changelog fragments:\n" + errors.joinToString("\n"))
    }
    return SECTIONS.values
      .associateWith { fragments[it].orEmpty() }
      .filterValues { it.isNotEmpty() }
  }

  /**
   * Appends the fragments to the matching sections of the Unreleased section of the changelog file,
   * or of the section of the given released [version], then deletes them. Returns the consumed
   * fragments. Targeting a released version is for fragments cherry-picked onto a release branch
   * after the changelog section of that version was created, i.e. for later release candidates.
   *
   * Everything that can fail is checked before the changelog is written: invalid fragments, missing
   * headings and fragments that cannot be deleted leave all files untouched. The changelog is then
   * replaced atomically.
   */
  fun merge(directory: File, changelog: File, version: String? = null): List<File> {
    val fragments = collect(directory)
    val consumed = fragments.values.flatten()
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

    val remaining = consumed.filter { f -> runCatching { Files.delete(f.toPath()) }.isFailure }
    if (remaining.isNotEmpty()) {
      throw GradleException(
        "${changelog.name} was updated, but these fragments could not be deleted:\n" +
          remaining.joinToString("\n") +
          "\nDelete them manually. Do NOT run the merge again, it would add their entries twice.",
      )
    }
    // Empty section directories are not tracked by Git, so a failure to delete them is harmless
    SECTIONS.keys.forEach { File(directory, it).delete() }
    return consumed
  }

  internal fun appendFragments(
    changelog: List<String>,
    fragments: Map<String, List<File>>,
    version: String? = null,
  ): List<String> {
    val lines = changelog.toMutableList()
    val target = "## [${version ?: "Unreleased"}]"
    for ((section, files) in fragments) {
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
      val entries = files.flatMap { f ->
        f.readText().trim().lines().mapIndexed { i, line ->
          if (i == 0) "- $line" else if (line.isBlank()) "" else "  $line"
        }
      }
      lines.addAll(
        insertAt,
        (if (insertAt == heading + 1) listOf("") else listOf()) +
          entries +
          (if (insertAt == sectionEnd) listOf("") else listOf()),
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
    val order = SECTIONS.values.toList() + "Commits"
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
