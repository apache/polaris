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

package licenses

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * Validates that every direct dependency in [bundledArtifacts] (in "group:artifactId" form) has a
 * corresponding `* Maven group:artifact IDs:` line in the bundle LICENSE file.
 *
 * This mirrors the Maven-coordinate mention check in [LicenseFileValidation] for Quarkus
 * distribution artifacts, adapted for shadow-jar bundle artifacts such as the Spark plugin. It does
 * **not** validate prose attribution sections (e.g. "This binary artifact contains Guava") or
 * transitive contents inside fat runtime JARs such as `iceberg-spark-runtime`.
 *
 * The [allowedExtraArtifacts] set lets callers declare entries that are intentionally present in the
 * BUNDLE-LICENSE for a reason not reflected in the current build's resolved classpath — for example,
 * cross-Scala-version variants of the same artifact (e.g. the `_2.13` variant when building for
 * `_2.12`). These entries are excluded from the "superfluous" check so that a single shared
 * BUNDLE-LICENSE file can cover multiple build variants without triggering false-positive failures.
 */
@DisableCachingByDefault(because = "lightweight validation task, not worth caching")
abstract class BundleLicenseValidation : DefaultTask() {

  @get:InputFile abstract val bundleLicenseFile: RegularFileProperty

  /** Set of "group:artifactId" strings that the bundle jar contains. */
  @get:Input abstract val bundledArtifacts: SetProperty<String>

  /**
   * Optional set of "group:artifactId" strings that are intentionally present in the BUNDLE-LICENSE
   * but are not part of the current build's resolved classpath (e.g. cross-Scala-version variants).
   * Entries listed here are exempt from the "superfluous" check.
   */
  @get:Input abstract val allowedExtraArtifacts: SetProperty<String>

  @TaskAction
  fun validate() {
    val error =
      validateLicenseMentions(
        bundleLicenseFile.get().asFile.readText(),
        bundledArtifacts.get(),
        allowedExtraArtifacts.get(),
      )
    if (error != null) {
      throw GradleException("BUNDLE-LICENSE validation failed:$error")
    }
  }

  companion object {
    /** Returns a validation error message, or null if validation passes. */
    internal fun validateLicenseMentions(
      licenseText: String,
      bundledArtifacts: Set<String>,
      allowedExtraArtifacts: Set<String>,
    ): String? {
      val mentioned =
        licenseText
          .lines()
          .filter { it.startsWith(LicenseFileValidation.LICENSE_MENTION_PREFIX) }
          .map { it.removePrefix(LicenseFileValidation.LICENSE_MENTION_PREFIX).trim() }
          .toSet()

      val allKnown = bundledArtifacts + allowedExtraArtifacts
      val missing = bundledArtifacts.filter { it !in mentioned }.sorted()
      val superfluous = mentioned.filter { it !in allKnown }.sorted()

      val errors = StringBuilder()
      if (missing.isNotEmpty()) {
        errors.append("\nMissing entries in BUNDLE-LICENSE (add these):\n")
        missing.forEach { errors.append("  ${LicenseFileValidation.LICENSE_MENTION_PREFIX}$it\n") }
      }
      if (superfluous.isNotEmpty()) {
        errors.append("\nSuperfluous entries in BUNDLE-LICENSE (remove these):\n")
        superfluous.forEach { errors.append("  ${LicenseFileValidation.LICENSE_MENTION_PREFIX}$it\n") }
      }
      return errors.takeIf { it.isNotEmpty() }?.toString()
    }
  }
}
