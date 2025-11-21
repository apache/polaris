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

import org.gradle.api.Project
import org.gradle.kotlin.dsl.extra

/**
 * Container to memoize Git information retrieved via `git` command executions across all Gradle
 * projects.
 */
class GitInfo(val gitHead: String, val gitDescribe: String, private val rawLinkRef: String) {

  fun rawGithubLink(file: String): String =
    "https://raw.githubusercontent.com/apache/polaris/$rawLinkRef/$file"

  companion object {
    private fun execGit(rootProject: Project, vararg args: Any): String {
      val out =
        rootProject.providers
          .exec {
            executable = "git"
            args(args.toList())
          }
          .standardOutput
          .asText
          .get()
      return out.trim()
    }

    fun memoized(project: Project): GitInfo {
      val rootProject = project.rootProject
      return if (rootProject.extra.has("gitInfo")) {
        @Suppress("UNCHECKED_CAST")
        rootProject.extra["gitInfo"] as GitInfo
      } else {
        val isRelease =
          rootProject.hasProperty("release") || rootProject.hasProperty("jarWithGitInfo")
        val gitHead = execGit(rootProject, "rev-parse", "HEAD")
        val gitDescribe =
          if (isRelease)
            try {
              execGit(rootProject, "describe", "--tags")
            } catch (_: Exception) {
              execGit(rootProject, "describe", "--always", "--dirty")
            }
          else ""
        val rawLinkRef = if (isRelease) gitDescribe else "HEAD"
        val gitInfo = GitInfo(gitHead, gitDescribe, rawLinkRef)
        rootProject.extra["gitInfo"] = gitInfo
        return gitInfo
      }
    }
  }
}
