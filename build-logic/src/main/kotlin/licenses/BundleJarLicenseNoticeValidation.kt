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

import java.util.zip.ZipFile
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/**
 * Validates that a shadow bundle JAR contains top-level LICENSE and NOTICE entries (renamed from
 * BUNDLE-LICENSE and BUNDLE-NOTICE at package time).
 */
@DisableCachingByDefault(because = "lightweight validation task, not worth caching")
abstract class BundleJarLicenseNoticeValidation : DefaultTask() {

  @get:InputFile abstract val bundleJar: RegularFileProperty

  @TaskAction
  fun validate() {
    val jarFile = bundleJar.get().asFile
    val missing = mutableListOf<String>()
    ZipFile(jarFile).use { zip ->
      for (entryName in REQUIRED_ENTRIES) {
        val entry = zip.getEntry(entryName)
        if (entry == null) {
          missing.add(entryName)
          continue
        }
        if (entry.size == 0L) {
          missing.add("$entryName (empty)")
        }
      }
    }
    if (missing.isNotEmpty()) {
      throw GradleException(
        "Bundle JAR '${jarFile.name}' is missing required license files: ${missing.joinToString(", ")}"
      )
    }
  }

  companion object {
    private val REQUIRED_ENTRIES = listOf("LICENSE", "NOTICE")
  }
}
