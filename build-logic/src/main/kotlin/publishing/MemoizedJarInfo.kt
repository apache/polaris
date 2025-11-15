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
import org.gradle.kotlin.dsl.extra

/**
 * Helper class to generate Jar manifest attributes including Git commit SHA, Git describe, project
 * version and Java specification version.
 */
internal class MemoizedJarInfo {
  companion object {
    fun applyJarManifestAttributes(rootProject: Project, attribs: Attributes) {
      val props = jarManifestAttributes(rootProject)
      attribs.putAll(props)
    }

    private fun jarManifestAttributes(rootProject: Project): Map<String, String> {
      return if (rootProject.extra.has("gitReleaseInfo")) {
        @Suppress("UNCHECKED_CAST")
        rootProject.extra["gitReleaseInfo"] as Map<String, String>
      } else {
        val isRelease =
          rootProject.hasProperty("release") || rootProject.hasProperty("jarWithGitInfo")
        val gi = GitInfo.memoized(rootProject)
        val javaSpecificationVersion = System.getProperty("java.specification.version")

        val version = rootProject.version.toString()
        val info =
          mapOf(
            "Implementation-Version" to version,
            "Apache-Polaris-Version" to version,
            "Apache-Polaris-Is-Release" to isRelease.toString(),
            "Apache-Polaris-Build-Git-Head" to gi.gitHead,
            "Apache-Polaris-Build-Git-Describe" to gi.gitDescribe,
            "Apache-Polaris-Build-Java-Specification-Version" to javaSpecificationVersion,
          )
        rootProject.extra["gitReleaseInfo"] = info
        return info
      }
    }
  }
}
