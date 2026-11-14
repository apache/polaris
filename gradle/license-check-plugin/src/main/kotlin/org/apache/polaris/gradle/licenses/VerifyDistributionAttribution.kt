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

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class VerifyDistributionAttribution : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.NONE)
  abstract val licenseFile: RegularFileProperty

  @get:InputFile
  @get:PathSensitive(PathSensitivity.NONE)
  abstract val noticeFile: RegularFileProperty

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val deliveredLicense: ConfigurableFileCollection

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val deliveredNotice: ConfigurableFileCollection

  @get:OutputFile abstract val validationReport: RegularFileProperty

  @TaskAction
  fun verify() {
    val errors = mutableListOf<String>()
    mapOf(
        "LICENSE" to (licenseFile to deliveredLicense),
        "NOTICE" to (noticeFile to deliveredNotice),
      )
      .forEach { (name, inputs) ->
        val expected = inputs.first.get().asFile.readBytes()
        val delivered = inputs.second.files
        if (expected.toString(Charsets.UTF_8).isBlank()) errors.add("$name input is blank")
        if (delivered.size != 1) {
          errors.add("Expected exactly one distribution $name, found ${delivered.size}")
        } else if (!delivered.single().readBytes().contentEquals(expected)) {
          errors.add("Delivered $name differs from attribution input")
        }
      }
    validationReport.get().asFile.apply {
      parentFile.mkdirs()
      writeText(
        if (errors.isEmpty()) "Delivered LICENSE and NOTICE match attribution inputs\n"
        else errors.joinToString("\n", postfix = "\n"),
      )
    }
    if (errors.isNotEmpty())
      throw GradleException(
        "Distribution attribution validation failed; see ${validationReport.get().asFile}",
      )
  }
}
