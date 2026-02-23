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

import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault(because = "not worth caching")
abstract class LicenseNoticeMerge @Inject constructor(objectFactory: ObjectFactory) :
  DefaultTask() {
  @get:OutputDirectory
  val mergedLicenseNotice: DirectoryProperty =
    objectFactory
      .directoryProperty()
      .convention(project.layout.buildDirectory.dir("merged-license-notice"))

  @get:InputFiles abstract val sourceLicenseNotice: ConfigurableFileCollection

  @get:InputFile
  val licenseHeaderFile: RegularFileProperty =
    objectFactory.fileProperty().convention(project.layout.projectDirectory.file("LICENSE-HEADER"))

  @get:InputFile
  val noticeHeaderFile: RegularFileProperty =
    objectFactory.fileProperty().convention(project.layout.projectDirectory.file("NOTICE-HEADER"))

  @TaskAction
  fun mergeLicenseNotices() {
    val licenseBlocks = readLicenseBlocks()
    val noticeBlocks = readNoticeFiles()

    mergedLicenseNotice.file("LICENSE").get().asFile.writer().use { writer ->
      writer.write(licenseHeaderFile.get().asFile.readText())
      licenseBlocks.forEach { block ->
        writer.write("\n${LicenseFileValidation.SEPARATOR}\n\n")
        writer.write("${block.header}\n\n")
        writer.write("${block.dependencies.joinToString("\n")}\n\n")
        writer.write("${block.suffix}\n")
      }
      writer.write("\n${LicenseFileValidation.SEPARATOR}\n")
    }

    mergedLicenseNotice.file("NOTICE").get().asFile.writer().use { writer ->
      writer.write(noticeHeaderFile.get().asFile.readText())
      noticeBlocks.forEach { block ->
        writer.write("\n${LicenseFileValidation.SEPARATOR}\n\n")
        println(block)
        writer.write("${block}\n\n")
      }
      writer.write("\n${LicenseFileValidation.SEPARATOR}\n")
    }
  }

  class LicenseBlock(val header: String, val dependencies: List<String>, val suffix: String)

  private fun readLicenseBlocks(): Collection<LicenseBlock> {
    val errors = mutableListOf<String>()
    val blocks =
      sourceLicenseNotice
        .flatMap { it ->
          val license = it.resolve("LICENSE").readText()
          license
            .split("\n${LicenseFileValidation.SEPARATOR}\n")
            .map { it.trim() }
            .filterIndexed { i, s -> i > 0 && !s.isBlank() }
            .map {
              val lines = it.split("\n")
              val iter = lines.iterator()
              val header = iter.next()
              if (!iter.next().isBlank()) {
                errors.add(
                  "* Invalid line after license block header for '$header', expected an empty line"
                )
              }
              val dependencies = mutableListOf<String>()
              val suffix = StringBuilder()
              while (iter.hasNext()) {
                val ln = iter.next().trim()
                if (ln.startsWith(LicenseFileValidation.LICENSE_MENTION_PREFIX)) {
                  dependencies.add(ln)
                } else {
                  suffix.append(ln).append("\n")
                  while (iter.hasNext()) {
                    suffix.append(iter.next().trim()).append("\n")
                  }
                  break
                }
              }
              LicenseBlock(header, dependencies, suffix.toString().trim())
            }
        }
        .groupingBy { it.header }
        .reduce { key, accumulator, element ->
          if (accumulator.suffix != element.suffix) {
            errors.add(
              "* License information for '$key' differs across the imported LICENSE files:\n${accumulator.suffix}\n${element.suffix}\n"
            )
          }
          LicenseBlock(
            key,
            accumulator.dependencies.plus(element.dependencies).sorted().distinct(),
            accumulator.suffix,
          )
        }
        .values

    if (errors.isNotEmpty())
      throw GradleException("License information validation failed:\n" + errors.joinToString("\n"))

    return blocks
  }

  private fun readNoticeFiles(): Collection<String> =
    sourceLicenseNotice
      .flatMap { it ->
        it
          .resolve("NOTICE")
          .readText()
          .split("\n${LicenseFileValidation.SEPARATOR}\n")
          .map { it.trim() }
          .filter { it.isNotBlank() }
      }
      .distinct()
}
