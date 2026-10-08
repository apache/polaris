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

package org.apache.polaris.gradle.licenses

import groovy.json.JsonSlurper
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class VerifyLicenseAttribution : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseeReport: RegularFileProperty

  @get:InputFile @get:PathSensitive(PathSensitivity.RELATIVE) abstract val sbom: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseFile: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val noticeFile: RegularFileProperty

  @get:Input abstract val projectGroup: Property<String>

  @get:Input abstract val generatedComponentNames: SetProperty<String>

  @get:OutputFile abstract val validationReport: RegularFileProperty

  @TaskAction
  fun verify() {
    val licenseeCoordinates =
      (JsonSlurper().parse(licenseeReport.get().asFile) as List<*>)
        .map { it as Map<*, *> }
        .map { component ->
          coordinate(component["groupId"], component["artifactId"], component["version"])
        }
        .toSortedSet()

    val sbomComponents =
      (JsonSlurper().parse(sbom.get().asFile) as Map<*, *>)["components"] as? List<*>
        ?: throw GradleException("Shipped inventory does not contain components")
    val included = sbomComponents.map { it as Map<*, *> }.filter { it["scope"] != "excluded" }
    val generated = included.filter { (it["purl"] as? String)?.startsWith("pkg:maven/") != true }
    require(
      generated.all {
        it["name"] in generatedComponentNames.get() &&
          (it["purl"] as? String)?.startsWith("pkg:generic/${it["name"]}@") == true
      },
    ) {
      "SBOM contains unrecognized non-Maven components: ${generated.map { it["name"] }}"
    }
    val shippedCoordinates =
      (included - generated.toSet())
        .filter { component ->
          component["group"] != projectGroup.get()
        }
        .map { component ->
          coordinate(component["group"], component["name"], component["version"])
        }
        .toSortedSet()

    require(licenseeCoordinates.isNotEmpty() && shippedCoordinates.isNotEmpty()) {
      "Expected nonempty policy evidence and shipped Maven inventory"
    }

    val licenseMarkers =
      licenseFile
        .get()
        .asFile
        .readLines()
        .mapNotNull { line ->
          Regex("^\\* Maven group:artifact IDs: ([^:]+:[^:]+)$")
            .matchEntire(line)
            ?.groupValues
            ?.get(1)
        }
        .toSortedSet()
    val shippedGroupArtifacts = shippedCoordinates.map { it.substringBeforeLast(':') }.toSortedSet()
    val missingLicensee = shippedCoordinates - licenseeCoordinates
    val missingMarkers = shippedGroupArtifacts - licenseMarkers
    val licenseeOnly = licenseeCoordinates - shippedCoordinates
    val noticeExists = noticeFile.get().asFile.readText().isNotBlank()

    val report = buildString {
      appendLine("External shipped Maven coordinates: ${shippedCoordinates.size}")
      appendLine("Licensee artifacts: ${licenseeCoordinates.size}")
      appendLine("LICENSE Maven group:artifact markers: ${licenseMarkers.size}")
      appendLine("NOTICE is non-empty: $noticeExists")
      appendSection("Shipped components not checked by Licensee", missingLicensee)
      appendSection("Shipped components missing LICENSE markers", missingMarkers)
      appendSection("Licensee artifacts not present in the shipped inventory", licenseeOnly)
      appendSection(
        "Generated files outside Maven inventory",
        generated.map { it["name"].toString() }.toSortedSet(),
      )
    }
    validationReport.get().asFile.apply {
      parentFile.mkdirs()
      writeText(report)
    }

    if (!noticeExists || missingLicensee.isNotEmpty() || missingMarkers.isNotEmpty()) {
      throw GradleException(
        "License attribution validation failed; see ${validationReport.get().asFile}",
      )
    }
  }

  private fun coordinate(group: Any?, artifact: Any?, version: Any?): String {
    require(
      group is String &&
        group.isNotBlank() &&
        artifact is String &&
        artifact.isNotBlank() &&
        version is String &&
        version.isNotBlank(),
    ) {
      "Expected Maven component with group, artifact, and version"
    }
    return "$group:$artifact:$version"
  }

  private fun StringBuilder.appendSection(title: String, values: Collection<String>) {
    appendLine()
    appendLine("$title (${values.size}):")
    values.forEach { appendLine("- $it") }
  }
}
