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

package asf

import groovy.json.JsonException
import groovy.json.JsonSlurper
import java.io.FileNotFoundException
import java.net.URI
import org.gradle.api.Project
import org.gradle.kotlin.dsl.extra

class AsfProject(
  val apacheId: String,
  val name: String,
  val description: String,
  val website: String,
  val repository: String,
  val licenseUrl: String,
  val bugDatabase: String,
  val inceptionYear: Int,
) {
  companion object {

    fun memoized(project: Project, asfName: String): AsfProject {
      val rootProject = project.rootProject
      return if (rootProject.extra.has("asfProject")) {
        unsafeCast(rootProject.extra["asfProject"]) as AsfProject
      } else {
        val asfProject = fetchProjectInformation(asfName)
        rootProject.extra["asfProject"] = asfProject
        return asfProject
      }
    }

    internal fun <T : Any> unsafeCast(o: Any?): T {
      @Suppress("UNCHECKED_CAST")
      return o as T
    }

    internal fun <T : Any> parseJson(urlStr: String): T {
      val url = URI(urlStr).toURL()

      val headers = mutableMapOf<String, String>()
      if (url.host == "api.github.com" && url.protocol == "https") {
        val githubToken = System.getenv("GITHUB_TOKEN")
        if (githubToken != null) {
          // leverage the GH token to benefit from higher rate limits
          headers["Authorization"] = "Bearer $githubToken"
          // recommended for GH API requests
          headers["X-GitHub-Api-Version"] = "2022-11-28"
          headers["Accept"] = "application/vnd.github+json"
        }
      }

      // See org.codehaus.groovy.runtime.ResourceGroovyMethods.newReader(URL, Map)
      val params = mapOf("requestProperties" to headers)

      val slurper = JsonSlurper()
      var attempt = 0
      while (true) {
        try {
          return unsafeCast(slurper.parse(params, url)) as T
        } catch (e: JsonException) {
          if (e.cause is FileNotFoundException) {
            throw e
          }
          if (attempt == 5) {
            throw e
          }
          Thread.sleep(1000L)
        }
        attempt++
      }
    }

    /** Retrieves the project name, for example `Polaris` using the lower-case project ID. */
    internal fun fetchAsfProjectName(apacheId: String): String {
      val project = projectMap(apacheId)
      val isPodlingCurrent = project.containsKey("podling") && project["podling"] == "current"
      if (isPodlingCurrent) {
        val podling = podlingMap(apacheId)
        return podling["name"] as String
      } else {
        // top-level-project
        val committee = projectCommitteeMap(apacheId)
        return committee["display_name"] as String
      }
    }

    internal fun projectCommitteeMap(apacheId: String): Map<String, Any> {
      val committeesAll: Map<String, Map<String, Any>> =
        parseJson("https://whimsy.apache.org/public/committee-info.json")
      val committees = unsafeCast<Map<String, Map<String, Any>>>(committeesAll["committees"])
      return unsafeCast(committees[apacheId])
    }

    internal fun projectMap(apacheId: String): Map<String, Any> {
      val projectsAll: Map<String, Map<String, Any>> =
        parseJson("https://whimsy.apache.org/public/public_ldap_projects.json")
      val projects = unsafeCast<Map<String, Map<String, Any>>>(projectsAll["projects"])
      val project =
        projects[apacheId]
          ?: throw IllegalArgumentException(
            "No project '$apacheId' found in https://whimsy.apache.org/public/public_ldap_projects.json"
          )
      return project
    }

    internal fun podlingMap(apacheId: String): Map<String, Any> {
      val podlingsAll: Map<String, Map<String, Any>> =
        parseJson("https://whimsy.apache.org/public/public_podlings.json")
      val podlings = unsafeCast<Map<String, Map<String, Any>>>(podlingsAll["podling"])
      val podling =
        podlings[apacheId]
          ?: throw IllegalArgumentException(
            "No podling '$apacheId' found in https://whimsy.apache.org/public/public_podlings.json"
          )
      return podling
    }

    internal fun fetchProjectInformation(apacheId: String): AsfProject {
      val project = projectMap(apacheId)
      val isPodlingCurrent = project.containsKey("podling") && project["podling"] == "current"

      val inceptionYear =
        (project["createTimestamp"] as String).subSequence(0, 4).toString().toInt()

      val projectName: String
      val description: String
      val website: String
      val repository: String
      val licenseUrl: String
      val bugDatabase: String
      if (isPodlingCurrent) {
        val podling = podlingMap(apacheId)
        projectName = podling["name"] as String
        description = podling["description"] as String
        val podlingStatus = unsafeCast(podling["podlingStatus"]) as Map<String, Any>
        website = podlingStatus["website"] as String
        // No repository for podlings??
        repository = "https://github.com/apache/$apacheId.git"
        bugDatabase = "https://github.com/apache/$apacheId/issues"
        licenseUrl = "https://www.apache.org/licenses/LICENSE-2.0.txt"
      } else {
        // top-level-project
        val tlpPrj: Map<String, Any> =
          parseJson("https://projects.apache.org/json/projects/$apacheId.json")
        website = tlpPrj["homepage"] as String
        repository = (unsafeCast(tlpPrj["repository"]) as List<String>)[0]
        bugDatabase = tlpPrj["bug-database"] as String
        licenseUrl = tlpPrj["license"] as String

        val committee = projectCommitteeMap(apacheId)
        projectName = committee["display_name"] as String
        description = committee["description"] as String
      }

      return AsfProject(
        apacheId,
        projectName,
        description,
        website,
        repository,
        licenseUrl,
        bugDatabase,
        inceptionYear,
      )
    }
  }
}
