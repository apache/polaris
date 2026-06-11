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

import java.io.ByteArrayOutputStream
import javax.inject.Inject
import org.gradle.api.Project
import org.gradle.api.provider.Property
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters
import org.gradle.process.ExecOperations

/**
 * Container to memoize Git information retrieved via `git` command executions across all Gradle
 * projects.
 */
class GitInfo(val gitHead: String, val gitDescribe: String, private val rawLinkRef: String) {

  fun rawGithubLink(file: String): String =
    "https://raw.githubusercontent.com/apache/polaris/$rawLinkRef/$file"

  companion object {
    fun memoized(project: Project): GitInfo {
      val rootProject = project.rootProject
      val isRelease =
        rootProject.hasProperty("release") || rootProject.hasProperty("jarWithGitInfo")
      val service =
        project.gradle.sharedServices.registerIfAbsent(
          "gitInfo-$isRelease",
          GitInfoService::class.java,
        ) {
          parameters.isRelease.set(isRelease)
        }
      return service.get().gitInfo
    }
  }
}

abstract class GitInfoService : BuildService<GitInfoService.Parameters> {
  interface Parameters : BuildServiceParameters {
    val isRelease: Property<Boolean>
  }

  @get:Inject abstract val execOperations: ExecOperations

  val gitInfo: GitInfo by lazy {
    val isRelease = parameters.isRelease.get()
    val gitHead = execGit("rev-parse", "HEAD")
    val gitDescribe =
      if (isRelease) {
        try {
          execGit("describe", "--tags")
        } catch (_: Exception) {
          execGit("describe", "--always", "--dirty")
        }
      } else {
        ""
      }
    val rawLinkRef = if (isRelease) gitDescribe else "HEAD"
    GitInfo(gitHead, gitDescribe, rawLinkRef)
  }

  private fun execGit(vararg args: String): String {
    val out = ByteArrayOutputStream()
    execOperations.exec {
      commandLine(listOf("git") + args)
      standardOutput = out
    }
    return out.toString().trim()
  }
}
