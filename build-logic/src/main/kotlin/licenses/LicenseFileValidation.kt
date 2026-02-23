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

import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.ProjectData
import com.github.jk1.license.filter.DependencyFilter
import java.io.File
import org.gradle.api.GradleException

/**
 * Validates that all dependencies with MIT/BSD/Go/UPL/ISC licenses, and Apache license, are
 * mentioned in the `LICENSE` file.
 */
class LicenseFileValidation : DependencyFilter {
  companion object {
    const val LICENSE_MENTION_PREFIX = "* Maven group:artifact IDs: "
    const val SEPARATOR =
      "--------------------------------------------------------------------------------"
  }

  fun doesNeedApacheMention(licenses: List<String?>): Boolean =
    licenses.filterNotNull().any { it.contains("Apache") }

  override fun filter(data: ProjectData?): ProjectData {
    data!!

    val licenseFile = data.project.file("distribution/LICENSE").readText()

    val licenseReport = data.project.extensions.getByType(LicenseReportExtension::class.java)

    val missingApacheMentions = mutableSetOf<String>()
    val missingFullMentions = mutableMapOf<String, String>()

    val allDependenciesGroupArtifactIds =
      data.allDependencies.map { "${it.group}:${it.name}" }.toSet()
    val superfluousDependencies =
      licenseFile
        .lines()
        .filter { it.startsWith(LICENSE_MENTION_PREFIX) }
        .map { it.substring(LICENSE_MENTION_PREFIX.length) }
        .filter { !allDependenciesGroupArtifactIds.contains(it.trim()) }

    data.allDependencies.forEach { mod ->
      val licenses =
        (mod.manifests.map { it.license } +
            mod.licenseFiles.flatMap { it.fileDetails }.map { it.license } +
            mod.poms.flatMap { it.licenses }.map { it.name })
          .distinct()

      val groupModule = "${mod.group}:${mod.name}"
      val groupModuleRegex =
        "^\\Q* Maven group:artifact IDs: $groupModule\\E$".toRegex(RegexOption.MULTILINE)
      if (!groupModuleRegex.containsMatchIn(licenseFile)) {
        if (doesNeedApacheMention(licenses)) {
          missingApacheMentions.add(groupModule)
        } else {
          missingFullMentions[groupModule] =
            """
            $SEPARATOR
            This product bundles ...<fill-in-name>.

            * Maven group:artifact IDs: $groupModule

            ${mod.licenseFiles.asSequence()
                .flatMap { it.fileDetails }
                .filter { it.file != null }
                .map { it.file }
                .map { File("${licenseReport.absoluteOutputDir}/$it").readText().trim() }
                .distinct()
                .joinToString("\n") { "\n\n$it\n" }
            }
            """
              .trimIndent()
        }
      }
    }

    val missingError = StringBuilder()
    if (!missingApacheMentions.isEmpty()) {
      missingError.append(
        """
          
          
          Missing Apache License mentions:
          --------------------------------
          ${missingApacheMentions.sorted().joinToString("\n") { "$LICENSE_MENTION_PREFIX$it" }}
          """
          .trimIndent()
      )
    }
    if (!missingFullMentions.isEmpty()) {
      missingError.append(
        """
          
          
          Missing full license mentions:
          ------------------------------
          ${missingFullMentions.toSortedMap().values.joinToString("\n") { "$LICENSE_MENTION_PREFIX$it" }}
          """
          .trimIndent()
      )
    }
    if (!superfluousDependencies.isEmpty()) {
      missingError.append(
        """
          
          
          Superfluous license mentions, should be removed:
          ------------------------------------------------
          ${superfluousDependencies.sorted().joinToString("\n") { "$LICENSE_MENTION_PREFIX$it" }}
          """
          .trimIndent()
      )
    }
    if (!missingError.isEmpty()) {
      throw GradleException(
        "License information for the following artifacts is missing in the root LICENSE file: $missingError"
      )
    }

    return data
  }
}
