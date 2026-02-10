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

package openapi

import java.io.ByteArrayOutputStream
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.openapitools.openapidiff.core.OpenApiCompare
import org.openapitools.openapidiff.core.compare.OpenApiDiffOptions
import org.openapitools.openapidiff.core.output.ConsoleRender
import org.openapitools.openapidiff.core.output.JsonRender
import org.openapitools.openapidiff.core.output.MarkdownRender

abstract class OpenApiCompatibilityCheckTask @Inject constructor(objectFactory: ObjectFactory) :
  DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  val referenceSpec = objectFactory.fileProperty()

  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  val currentSpec = objectFactory.fileProperty()

  @get:OutputDirectory
  val reportDir =
    objectFactory
      .directoryProperty()
      .convention(project.layout.buildDirectory.dir("reports/openapi"))

  @TaskAction
  fun check() {
    val reportDir = reportDir.get()
    reportDir.asFile.deleteRecursively()
    reportDir.asFile.mkdirs()

    val referenceSpecFile = referenceSpec.get().asFile
    val currentSpecFile = currentSpec.get().asFile

    logger.info(
      "Generating OpenAPI diff report of ${currentSpecFile.relativeTo(project.projectDir)} against reference ${referenceSpecFile.absolutePath}"
    )

    val diffResult =
      OpenApiCompare.fromFiles(
        referenceSpecFile,
        currentSpecFile,
        listOf(),
        OpenApiDiffOptions.builder().build(),
      )

    val jsonFile = reportDir.file("openapi-diff.json")
    val markdownFile = reportDir.file("openapi-diff.md")

    jsonFile.asFile.writer().use { JsonRender().render(diffResult, it) }
    markdownFile.asFile.writer().use { MarkdownRender().render(diffResult, it) }
    val consoleOutput = ByteArrayOutputStream()
    consoleOutput.writer().use { ConsoleRender().render(diffResult, it) }

    if (diffResult.isIncompatible) {
      logger.error("OpenAPI diff report generated in ${reportDir.asFile.absolutePath}")
      logger.error(consoleOutput.toString())
      throw GradleException(
        "OpenAPI spec is incompatible! See reports in ${reportDir.asFile.absolutePath}"
      )
    } else {
      logger.info("OpenAPI diff report generated in ${reportDir.asFile.absolutePath}")
      logger.info(consoleOutput.toString())
    }
  }
}
