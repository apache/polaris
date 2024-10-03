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

import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.ProjectData
import com.github.jk1.license.filter.DependencyFilter
import java.io.File
import org.gradle.api.GradleException

/**
 * Validates that all dependencies with MIT/BSD/Go/UPL/ISC licenses, and Apache
 * license, are mentioned in the `LICENSE` file.
 */
class LicenseFileValidation : DependencyFilter {
  val needsApacheLicenseMention = setOf("Apache")

  val needsFullLicenseMention = setOf("MIT", "BSD", "Go", "ISC", "Universal Permissive")

  fun doesNeedApacheMention(licenses: List<String?>): Boolean {
    for (license in licenses) {
      if (license != null) {
        if (needsApacheLicenseMention.any { license.contains(it) }) {
          return true
        }
      }
    }
    return false
  }

  fun doesNeedFullMention(licenses: List<String?>): Boolean {
    for (license in licenses) {
      if (license != null) {
        if (needsFullLicenseMention.any { license.contains(it) }) {
          return true
        }
      }
    }
    // no licenses !
    return true
  }

  override fun filter(data: ProjectData?): ProjectData {
    data!!

    val rootLicenseFile = data.project.rootProject.file("LICENSE").readText()

    val licenseReport = data.project.extensions.getByType(LicenseReportExtension::class.java)

    val missingApacheMentions = mutableSetOf<String>()
    val missingFullMentions = mutableMapOf<String, String>()

    data.allDependencies.forEach { mod ->
      val licenses =
        (mod.manifests.map { it.license } +
            mod.licenseFiles.flatMap { it.fileDetails }.map { it.license } +
            mod.poms.flatMap { it.licenses }.map { it.name })
          .distinct()

      val groupModule = "${mod.group}:${mod.name}"
      val groupModuleRegex = "^$groupModule$".toRegex(RegexOption.MULTILINE)
      if (!groupModuleRegex.containsMatchIn(rootLicenseFile)) {
        if (doesNeedApacheMention(licenses)) {
          missingApacheMentions.add(groupModule)
        } else if (doesNeedFullMention(licenses)) {
            missingFullMentions[groupModule] = """
            ---
            $groupModule

            ${mod.licenseFiles.flatMap { it.fileDetails }.filter { it.file != null }.map { it.file }
              .map { File("${licenseReport.absoluteOutputDir}/$it").readText().trim() }
              .distinct()
              .map { "\n\n$it\n" }
              .joinToString("\n")
            }
            """.trimIndent()
        }
      }
    }

    val missingError = StringBuilder()
    if (!missingApacheMentions.isEmpty()) {
      missingError.append("\n\nMissing Apache License mentions:")
      missingError.append("\n--------------------------------\n")
      missingApacheMentions.sorted().forEach {
        missingError.append("\n$it")
      }
    }
    if (!missingFullMentions.isEmpty()) {
      missingError.append("\n\nMissing full license mentions:")
      missingError.append("\n------------------------------\n")
      missingFullMentions.toSortedMap().values.forEach {
        missingError.append("\n$it")
      }
    }
    if (!missingApacheMentions.isEmpty() || !missingFullMentions.isEmpty()) {

      throw GradleException(
        "License information for the following artifacts is missing in the root LICENSE file: $missingError"
      )
    }

    return data
  }
}
