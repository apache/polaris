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

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.UntrackedTask
import org.gradle.api.tasks.options.Option

/** Validates the changelog fragments, see [ChangelogFragments.collect]. */
@CacheableTask
abstract class CheckChangelogFragmentsTask : DefaultTask() {

  @get:InputDirectory
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val fragmentsDirectory: DirectoryProperty

  /**
   * Marker written only after the fragments have been validated successfully, listing them for
   * human review. Gradle needs an output for its up-to-date checks and the build cache, but
   * freshness depends on the inputs only. The paths are relative to [fragmentsDirectory], so that
   * the content is the same when the result is restored from the build cache in another checkout.
   * Should other tasks ever depend on the validation, we'd have to expose a value derived from the
   * fragment files and their contents instead.
   */
  @get:OutputFile abstract val successMarker: RegularFileProperty

  @TaskAction
  fun check() {
    val directory = fragmentsDirectory.get().asFile
    val fragments = ChangelogFragments.collect(directory)
    successMarker
      .get()
      .asFile
      .writeText(
        "Validated ${fragments.size} changelog fragment(s):\n" +
          fragments.joinToString("") {
            "${it.file.relativeTo(directory).invariantSeparatorsPath}: " +
              "${it.sections.keys.joinToString(", ")}\n"
          },
      )
  }
}

/** Merges the changelog fragments into the changelog file, see [ChangelogFragments.merge]. */
@UntrackedTask(
  because =
    "Rewrites its own input (the changelog file) and deletes the fragments it consumes, so it must " +
      "never be considered up to date or restored from the build cache",
)
abstract class MergeChangelogFragmentsTask : DefaultTask() {

  @get:Internal abstract val fragmentsDirectory: DirectoryProperty

  @get:Internal abstract val changelogFile: RegularFileProperty

  /** The released version whose section receives the fragments, instead of Unreleased. */
  @get:Internal
  @get:Option(
    option = "release-version",
    description = "Merge into the section of this released version instead of Unreleased",
  )
  abstract val releaseVersion: Property<String>

  @TaskAction
  fun merge() {
    val consumed =
      ChangelogFragments.merge(
        fragmentsDirectory.get().asFile,
        changelogFile.get().asFile,
        releaseVersion.orNull,
      )
    logger.lifecycle("Merged ${consumed.size} changelog fragment(s) into CHANGELOG.md")
  }
}

/**
 * Merges the changelog fragments into the changelog file, and turns its Unreleased section into the
 * section of the new release, see [ChangelogFragments.promote].
 */
@UntrackedTask(
  because =
    "Rewrites its own input (the changelog file) and deletes the fragments it consumes, so it must " +
      "never be considered up to date or restored from the build cache",
)
abstract class ReleaseChangelogTask : DefaultTask() {

  @get:Internal abstract val fragmentsDirectory: DirectoryProperty

  @get:Internal abstract val changelogFile: RegularFileProperty

  /** The version of the new release. */
  @get:Internal
  @get:Option(option = "release-version", description = "The version of the new release")
  abstract val releaseVersion: Property<String>

  @TaskAction
  fun release() {
    val version =
      releaseVersion.orNull ?: throw GradleException("The release version option is required")
    val consumed =
      ChangelogFragments.merge(
        fragmentsDirectory.get().asFile,
        changelogFile.get().asFile,
        version,
        release = true,
      )
    logger.lifecycle(
      "Created the CHANGELOG.md section of $version, with ${consumed.size} changelog fragment(s)",
    )
  }
}
