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

import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.java.archives.Attributes
import org.gradle.kotlin.dsl.extra

/**
 * Container to memoize Git information retrieved via `git` command executions across all Gradle
 * projects. Jar release artifacts get some attributes added to the jar manifest, which can be quite
 * useful for released jars.
 */
internal class MemoizedGitInfo {
  companion object {
    private fun execProc(rootProject: Project, cmd: String, vararg args: Any): String {
      var out =
        rootProject.providers
          .exec {
            executable = cmd
            args(args.toList())
          }
          .standardOutput
          .asText
          .get()
      return out.trim()
    }

    fun gitInfo(rootProject: Project, attribs: Attributes) {
      val props = gitInfo(rootProject)
      attribs.putAll(props)
    }

    fun gitInfo(rootProject: Project): Map<String, String> {
      return if (rootProject.extra.has("gitReleaseInfo")) {
        @Suppress("UNCHECKED_CAST")
        rootProject.extra["gitReleaseInfo"] as Map<String, String>
      } else {
        val isRelease = rootProject.hasProperty("release")
        val gitHead = execProc(rootProject, "git", "rev-parse", "HEAD")
        val gitDescribe =
          if (isRelease) {
            try {
              execProc(rootProject, "git", "describe", "--tags")
            } catch (e: Exception) {
              throw GradleException("'git describe --tags' failed - no Git tag?", e)
            }
          } else {
            execProc(rootProject, "git", "describe", "--always", "--dirty")
          }
        val timestamp = execProc(rootProject, "date", "+%Y-%m-%d-%H:%M:%S%:z")
        val system = execProc(rootProject, "uname", "-a")
        val javaVersion = System.getProperty("java.version")

        val info =
          mapOf(
            "Apache-Polaris-Version" to rootProject.version.toString(),
            "Apache-Polaris-Is-Release" to isRelease.toString(),
            "Apache-Polaris-Build-Git-Head" to gitHead,
            "Apache-Polaris-Build-Git-Describe" to gitDescribe,
            "Apache-Polaris-Build-Timestamp" to timestamp,
            "Apache-Polaris-Build-System" to system,
            "Apache-Polaris-Build-Java-Version" to javaVersion
          )
        rootProject.extra["gitReleaseInfo"] = info
        return info
      }
    }
  }
}
