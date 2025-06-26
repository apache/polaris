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

import org.gradle.api.Project
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.file.SourceDirectorySet
import org.gradle.api.provider.Property
import org.gradle.api.provider.SetProperty

abstract class CopiedCodeCheckerExtension(private val project: Project) {
  /**
   * Per-project set of additional directories to scan.
   *
   * This property is _not_ propagated to subprojects.
   */
  val scanDirectories =
    project.objects.domainObjectContainer(
      SourceDirectorySet::class.java,
      { name -> project.objects.sourceDirectorySet(name, name) },
    )

  /**
   * By default, this plugin scans all files. The content types that match the regular expression of
   * this property are excluded, unless a content-type matches one of the regular expressions in
   * [CopiedCodeCheckerExtension.includedContentTypePatterns].
   *
   * See [CopiedCodeCheckerExtension.addDefaultContentTypes],
   * [CopiedCodeCheckerExtension.includedContentTypePatterns],
   * [CopiedCodeCheckerExtension.includeUnrecognizedContentType],
   * [CopiedCodeCheckerExtension.includedContentTypePatterns].
   */
  abstract val excludedContentTypePatterns: SetProperty<String>
  /**
   * By default, this plugin scans all files. The content types that match the regular expression of
   * the [copiedcode.CopiedCodeCheckerExtension.excludedContentTypePatterns] property are excluded,
   * unless a content-type matches one of the regular expressions in this property.
   *
   * See [CopiedCodeCheckerExtension.addDefaultContentTypes],
   * [CopiedCodeCheckerExtension.excludedContentTypePatterns],
   * [CopiedCodeCheckerExtension.includeUnrecognizedContentType],
   * [CopiedCodeCheckerExtension.includedContentTypePatterns].
   */
  abstract val includedContentTypePatterns: SetProperty<String>

  /**
   * If a content-type could not be detected, this property, which defaults to `true`, is consulted.
   *
   * See [CopiedCodeCheckerPlugin] for details.
   */
  abstract val includeUnrecognizedContentType: Property<Boolean>

  /**
   * The magic "word", if present in a file, meaning "this file has been copied".
   *
   * A file is considered as "copied" must contain this magic word. "Word" means that the value must
   * be surrounded by regular expression word boundaries (`\b`).
   */
  abstract val magicWord: Property<String>

  /**
   * License file to check, configured on the root project. See [CopiedCodeCheckerPlugin] for
   * details.
   */
  abstract val licenseFile: RegularFileProperty

  /** Recommended to use, adds known and used binary content types. */
  fun addDefaultContentTypes(): CopiedCodeCheckerExtension {
    // Exclude all images
    excludedContentTypePatterns.add("image/.*")
    // But include images built in XML (e.g. image/svg+xml)
    includedContentTypePatterns.add("\\+xml")

    return this
  }

  init {
    includeUnrecognizedContentType.convention(true)
    magicWord.convention(DEFAULT_MAGIC_WORD)
  }

  companion object {
    // String manipulation is intentional - otherwise this source file would be considered as
    // "copied".
    val DEFAULT_MAGIC_WORD = "_CODE_COPIED_TO_POLARIS".substring(1)
  }
}
