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

  /** Marker written only after the fragments have been validated successfully. */
  @get:OutputFile abstract val successMarker: RegularFileProperty

  @TaskAction
  fun check() {
    ChangelogFragments.collect(fragmentsDirectory.get().asFile)
    successMarker.get().asFile.writeText("ok\n")
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
