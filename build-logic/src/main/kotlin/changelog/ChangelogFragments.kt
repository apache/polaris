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
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * Base class for tasks handling the changelog fragments in `changelog.d`, see
 * `changelog.d/README.md`.
 */
@DisableCachingByDefault
abstract class ChangelogFragmentsTask : DefaultTask() {

  @get:Internal abstract val fragmentsDirectory: DirectoryProperty

  /** Returns the fragment files per section heading, in order, failing on invalid fragments. */
  protected fun collectFragments(): Map<String, List<File>> {
    val errors = mutableListOf<String>()
    val fragments = mutableMapOf<String, List<File>>()
    (fragmentsDirectory.get().asFile.listFiles() ?: emptyArray()).forEach { f ->
      val section = SECTIONS[f.name]
      if (f.isDirectory && section != null) {
        fragments[section] = f.listFiles()!!.sortedBy { it.name }
      } else if (f.name != "README.md") {
        errors.add("$f: unknown section, must be one of ${SECTIONS.keys}")
      }
    }
    fragments.values.flatten().forEach { f ->
      when {
        !f.isFile || !FRAGMENT_NAME.matches(f.name) ->
          errors.add("$f: file name must match NN-short-title.md, for example 99-my-change.md")
        f.readText().isBlank() -> errors.add("$f: file is empty")
        LIST_MARKER.containsMatchIn(f.readText().trim()) ->
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

  companion object {
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
  }
}

/** Validates the changelog fragments. */
@DisableCachingByDefault
abstract class CheckChangelogFragmentsTask : ChangelogFragmentsTask() {
  @TaskAction
  fun check() {
    collectFragments()
  }
}

/**
 * Appends the changelog fragments to the matching sections of the Unreleased section of the
 * changelog file, then deletes them.
 */
@DisableCachingByDefault
abstract class MergeChangelogFragmentsTask : ChangelogFragmentsTask() {

  @get:Internal abstract val changelogFile: RegularFileProperty

  @TaskAction
  fun merge() {
    val fragments = collectFragments()
    val changelog = changelogFile.get().asFile
    val lines = changelog.readLines().toMutableList()
    for ((section, files) in fragments) {
      val unreleased = lines.indexOf("## [Unreleased]")
      if (unreleased < 0) throw GradleException("Section '## [Unreleased]' not found")
      val unreleasedEnd =
        (unreleased + 1 until lines.size).firstOrNull { lines[it].startsWith("## ") } ?: lines.size
      val heading =
        (unreleased + 1 until unreleasedEnd).firstOrNull { lines[it] == "### $section" }
          ?: throw GradleException("Section '### $section' not found under '## [Unreleased]'")
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
    changelog.writeText(lines.joinToString("\n", postfix = "\n"))
    fragments.values.flatten().forEach { it.delete() }
    fragmentsDirectory.get().asFile.listFiles()!!.filter { it.isDirectory }.forEach { it.delete() }
    logger.lifecycle(
      "Merged ${fragments.values.sumOf { it.size }} changelog fragment(s) into ${changelog.name}",
    )
  }

  private companion object {
    val LINK_REFERENCE = Regex("\\[[^]]+]:.*")
  }
}
