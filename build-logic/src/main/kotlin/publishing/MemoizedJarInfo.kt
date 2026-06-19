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

package publishing

import org.gradle.api.Project
import org.gradle.api.java.archives.Attributes

/**
 * Helper class to generate Jar manifest attributes including project version and Java specification
 * version. Git information like the commit SHA and Git describe output are only included for
 * release builds, or if explicitly requested.
 */
internal class MemoizedJarInfo {
  companion object {
    fun applyJarManifestAttributes(project: Project, attribs: Attributes) {
      val props = jarManifestAttributes(project)
      attribs.putAll(props)
    }

    private fun jarManifestAttributes(project: Project): Map<String, String> {
      val version = project.version.toString()
      val javaSpecificationVersion =
        project.providers.systemProperty("java.specification.version").get()
      val includeGitInformation =
        project.providers.gradleProperty("release").isPresent ||
          project.providers.gradleProperty("jarWithGitInfo").isPresent

      return if (includeGitInformation) {
        val gi = GitInfo.memoized(project)
        mapOf(
          "Implementation-Version" to version,
          "Apache-Polaris-Version" to version,
          "Apache-Polaris-Is-Release" to "true",
          "Apache-Polaris-Build-Git-Head" to gi.gitHead,
          "Apache-Polaris-Build-Git-Describe" to gi.gitDescribe,
          "Apache-Polaris-Build-Java-Specification-Version" to javaSpecificationVersion,
        )
      } else {
        // Not adding Git information here to keep Gradle's up-to-date functionality intact.
        // Varying information in the manifest would change the MANIFEST.MF file and the jar.
        // If the output changes, the input of dependent tasks is no longer up-to-date and would
        // need to be rebuilt.
        // This would render the Gradle build-cache ineffective for every Git commit,
        // especially in CI, leading to unnecessary long builds.
        mapOf(
          "Implementation-Version" to version,
          "Apache-Polaris-Version" to version,
          "Apache-Polaris-Is-Release" to "false",
          "Apache-Polaris-Build-Java-Specification-Version" to javaSpecificationVersion,
        )
      }
    }
  }
}
