/*
 * Copyright (C) 2024 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package copiedcode

import java.nio.file.Files
import java.util.regex.Pattern
import javax.inject.Inject
import kotlin.collections.joinToString
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.component.SoftwareComponentFactory
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.provider.Property
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * This plugin identifies files that have been originally copied from another project.
 *
 * Configuration is done using the [CopiedCodeCheckerExtension], available under the name
 * `copiedCodeChecks`.
 *
 * Such files need to contain a magic word, see [CopiedCodeCheckerExtension.magicWord].
 *
 * This plugin scans all source directories configured in the project's [SourceDirectorySet]. Files
 * in the project's build directory are always excluded.
 *
 * By default, this plugin scans all files. There is a convenience function to exclude known binary
 * types, see [CopiedCodeCheckerExtension.addDefaultContentTypes]. The
 * [CopiedCodeCheckerExtension.excludedContentTypePatterns] is checked first against a detected
 * content type. If a content-type's excluded, the
 * [CopiedCodeCheckerExtension.includedContentTypePatterns] is consulted. If a content-type could
 * not be detected, the property [CopiedCodeCheckerExtension.includeUnrecognizedContentType], which
 * defaults to `true`, is consulted.
 *
 * Each Gradle project has its own instance of the [CopiedCodeCheckerExtension], the extension of
 * the root project serves default values, except for [CopiedCodeCheckerExtension.scanDirectories]]
 *
 * The license file to check is configured via [CopiedCodeCheckerExtension.licenseFile]. Files must
 * be mentioned using the relative path from the root directory, with a trailing `* ` (star and
 * space).
 */
@Suppress("unused")
class CopiedCodeCheckerPlugin
@Inject
constructor(private val softwareComponentFactory: SoftwareComponentFactory) : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      val extension =
        extensions.create("copiedCodeChecks", CopiedCodeCheckerExtension::class.java, project)

      if (rootProject == this) {
        // Apply this plugin to all projects
        afterEvaluate { subprojects { plugins.apply(CopiedCodeCheckerPlugin::class.java) } }

        tasks.register(
          CHECK_COPIED_CODE_MENTIONS_EXIST_TASK_NAME,
          CheckCopiedCodeMentionsExistTask::class.java,
        ) {
          licenseFile.convention(extension.licenseFile)
          rootDirectory.convention(layout.projectDirectory)
        }

        afterEvaluate {
          tasks.named("check").configure { dependsOn(CHECK_COPIED_CODE_MENTIONS_EXIST_TASK_NAME) }
        }
      } else {
        extension.excludedContentTypePatterns.convention(
          provider {
            rootProject.extensions
              .getByType(CopiedCodeCheckerExtension::class.java)
              .excludedContentTypePatterns
              .get()
          }
        )
        extension.includedContentTypePatterns.convention(
          provider {
            rootProject.extensions
              .getByType(CopiedCodeCheckerExtension::class.java)
              .includedContentTypePatterns
              .get()
          }
        )
        extension.includeUnrecognizedContentType.convention(
          provider {
            rootProject.extensions
              .getByType(CopiedCodeCheckerExtension::class.java)
              .includeUnrecognizedContentType
              .get()
          }
        )
        extension.licenseFile.convention(
          provider {
            rootProject.extensions
              .getByType(CopiedCodeCheckerExtension::class.java)
              .licenseFile
              .get()
          }
        )
      }

      val checkForCopiedCode =
        tasks.register(CHECK_FOR_COPIED_CODE_TASK_NAME, CheckForCopiedCodeTask::class.java) {
          licenseFile.convention(extension.licenseFile)
          rootDirectory.convention(rootProject.layout.projectDirectory)
          projectDirectory.convention(layout.projectDirectory)
          buildDirectory.convention(layout.buildDirectory)
          excludedContentTypePatterns.convention(extension.excludedContentTypePatterns)
          includedContentTypePatterns.convention(extension.includedContentTypePatterns)
          includeUnrecognizedContentType.convention(extension.includeUnrecognizedContentType)
          magicWord.convention(extension.magicWord)
        }

      extension.scanDirectories.configureEach {
        checkForCopiedCode.configure { sourceFiles.from(asFileTree) }
      }

      plugins.withType(JavaBasePlugin::class.java) {
        extensions.getByType(SourceSetContainer::class.java).configureEach {
          allSource.srcDirs
            .filter { sourceDir -> !sourceDir.startsWith(layout.buildDirectory.get().asFile) }
            .forEach { sourceDir ->
              checkForCopiedCode.configure { sourceFiles.from(fileTree(sourceDir)) }
            }
        }
      }

      afterEvaluate {
        tasks.named("check").configure { dependsOn(CHECK_FOR_COPIED_CODE_TASK_NAME) }
      }
    }

  companion object {
    private const val CHECK_FOR_COPIED_CODE_TASK_NAME = "checkForCopiedCode"
    private const val CHECK_COPIED_CODE_MENTIONS_EXIST_TASK_NAME = "checkCopiedCodeMentionsExist"
  }
}

@DisableCachingByDefault
abstract class CheckCopiedCodeMentionsExistTask : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseFile: RegularFileProperty

  @get:Internal abstract val rootDirectory: DirectoryProperty

  @TaskAction
  fun checkMentions() {
    val licenseFile = licenseFile.get().asFile
    val rootDirectory = rootDirectory.get().asFile
    val licenseFileRelative = licenseFile.relativeTo(rootDirectory).toString()

    logger.info("Checking whether files mentioned in the {} file exist", licenseFileRelative)

    val nonExistingMentions =
      licenseFile
        .readLines()
        .filter { line -> line.startsWith("* ") && line.length > 2 }
        .map { line -> line.substring(2) }
        .filter { relFilePath -> !rootDirectory.resolve(relFilePath).exists() }
        .sorted()

    if (nonExistingMentions.isNotEmpty()) {
      logger.error(
        """
        The following {} files mentioned in {} do not exist, fix the {} file.

        {}
        """
          .trimIndent(),
        nonExistingMentions.size,
        licenseFileRelative,
        licenseFileRelative,
        nonExistingMentions.joinToString("\n* ", "* "),
      )

      throw GradleException(
        "${nonExistingMentions.size} files mentioned in $licenseFileRelative do not exist, fix the $licenseFileRelative file."
      )
    }
  }
}

@DisableCachingByDefault
abstract class CheckForCopiedCodeTask : DefaultTask() {
  @get:InputFile
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val licenseFile: RegularFileProperty

  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val sourceFiles: ConfigurableFileCollection

  @get:Input abstract val excludedContentTypePatterns: SetProperty<String>

  @get:Input abstract val includedContentTypePatterns: SetProperty<String>

  @get:Input abstract val includeUnrecognizedContentType: Property<Boolean>

  @get:Input abstract val magicWord: Property<String>

  @get:Internal abstract val rootDirectory: DirectoryProperty

  @get:Internal abstract val projectDirectory: DirectoryProperty

  @get:Internal abstract val buildDirectory: DirectoryProperty

  @TaskAction
  fun checkForCopiedCode() {
    val licenseFile = licenseFile.get().asFile
    val rootDirectory = rootDirectory.get().asFile
    val projectDirectory = projectDirectory.get().asFile
    val licenseFileRelative = licenseFile.relativeTo(rootDirectory).toString()

    logger.info("Running copied code check against root project's {} file", licenseFileRelative)

    val includedPatterns = includedContentTypePatterns.get().map { Pattern.compile(it) }
    val excludedPatterns = excludedContentTypePatterns.get().map { Pattern.compile(it) }
    val includeUnknown = includeUnrecognizedContentType.get()

    val magicWord = magicWord.get()
    val magicWordPattern = Pattern.compile(".*\\b${magicWord}\\b.*")

    val mentionedFilesInLicense =
      licenseFile
        .readLines()
        .filter { line -> line.startsWith("* ") && line.length > 2 }
        .map { line -> line.substring(2) }
        .toSet()

    val buildDir = buildDirectory.get().asFile

    val unmentionedFiles =
      sourceFiles
        .filter { file -> !file.startsWith(buildDir) }
        .mapNotNull { file ->
          val projectRelativeFile = file.relativeTo(projectDirectory)
          val fileType = Files.probeContentType(file.toPath())
          logger.info("Checking file '{}' (probed content type: {})", projectRelativeFile, fileType)

          var r: String? = null

          var check = true
          if (fileType == null) {
            if (!includeUnknown) {
              logger.info("   ... unknown content type, skipping")
              check = false
            }
          } else {
            val excluded = excludedPatterns.any { pattern -> pattern.matcher(fileType).matches() }
            if (excluded) {
              val included = includedPatterns.any { pattern -> pattern.matcher(fileType).matches() }
              if (!included) {
                logger.info("   ... excluded and not included content type, skipping")
                check = false
              }
            }
          }

          if (check) {
            if (!file.readLines().any { s -> magicWordPattern.matcher(s).matches() }) {
              logger.info("   ... no magic word, not expecting an entry in {}", licenseFileRelative)
            } else {
              val relativeFilePath = file.relativeTo(rootDirectory).toString()
              if (mentionedFilesInLicense.contains(relativeFilePath)) {
                logger.info("   ... has magic word & mentioned in {}", licenseFileRelative)
              } else {
                // error (summary) logged below
                logger.info(
                  "The file '{}' has the {} marker, but is not mentioned in {}",
                  relativeFilePath,
                  magicWord,
                  licenseFileRelative,
                )
                r = relativeFilePath
              }
            }
          }

          r
        }
        .sorted()
        .toList()

    if (!unmentionedFiles.isEmpty()) {
      logger.error(
        """
        The following {} files have the {} marker but are not mentioned in {}, add those in an appropriate section.

        {}
        """
          .trimIndent(),
        unmentionedFiles.size,
        magicWord,
        licenseFileRelative,
        unmentionedFiles.joinToString("\n* ", "* "),
      )

      throw GradleException(
        "${unmentionedFiles.size} files with the $magicWord marker need to be mentioned in $licenseFileRelative. See the messages above."
      )
    }
  }
}
