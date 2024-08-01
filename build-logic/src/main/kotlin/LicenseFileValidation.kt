/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.github.jk1.license.LicenseReportExtension
import com.github.jk1.license.ProjectData
import com.github.jk1.license.filter.DependencyFilter
import java.io.File
import org.gradle.api.GradleException

/**
 * Validates that all dependencies with MIT/BSD/Go/UPL/ISC licenses, which do not have an Apache
 * license, are mentioned in the `LICENSE` file.
 */
class LicenseFileValidation : DependencyFilter {
  fun needsNoMention(license: String?): Boolean = license != null && (license.contains("Apache"))

  fun needsMention(license: String?): Boolean =
    license != null &&
      (license.contains("MIT") ||
        license.contains("BSD") ||
        license.contains("Go") ||
        license.contains("ISC") ||
        license.contains("Universal Permissive"))

  override fun filter(data: ProjectData?): ProjectData {
    data!!

    val rootLicenseFile = data.project.rootProject.file("LICENSE").readText()

    val licenseReport = data.project.extensions.getByType(LicenseReportExtension::class.java)

    val missing = mutableMapOf<String, String>()

    data.allDependencies.forEach { mod ->
      val licenses =
        (mod.manifests.map { it.license } +
            mod.licenseFiles.flatMap { it.fileDetails }.map { it.license } +
            mod.poms.flatMap { it.licenses }.map { it.name })
          .distinct()

      if (!licenses.any { needsNoMention(it) } && licenses.any { needsMention(it) }) {
        val groupModule = "${mod.group}:${mod.name}"
        if (!rootLicenseFile.contains(groupModule)) {
          missing.put(
            "${mod.group}:${mod.name}",
            """
            ---
            ${mod.group}:${mod.name}

            ${mod.licenseFiles.flatMap { it.fileDetails }.filter { it.file != null }.map { it.file }
              .map { File("${licenseReport.absoluteOutputDir}/$it").readText().trim() }
              .distinct()
              .map { "\n\n$it\n" }
              .joinToString("\n")
            }
            """
              .trimIndent()
          )
        }
      }
    }

    if (!missing.isEmpty()) {
      throw GradleException(
        "License information for the following artifacts is missing in the root LICENSE file: ${missing.map { it.value }.joinToString("\n")}"
      )
    }

    return data
  }
}
