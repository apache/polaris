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

import groovy.json.JsonException
import groovy.json.JsonSlurper
import groovy.util.Node
import groovy.util.NodeList
import java.io.File
import java.io.FileNotFoundException
import java.net.URI
import java.security.MessageDigest
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.component.ModuleComponentSelector
import org.gradle.api.artifacts.result.DependencyResult

internal fun findDependency(
  config: Configuration?,
  depGroup: String,
  depName: String
): DependencyResult? {
  if (config != null) {
    val depResult =
      config.incoming.resolutionResult.allDependencies.find { depResult ->
        val req = depResult.requested
        if (req is ModuleComponentSelector) req.group == depGroup && req.module == depName
        else false
      }
    return depResult
  }
  return null
}

internal fun xmlNode(node: Node?, child: String): Node? {
  val found = node?.get(child)
  if (found is NodeList) {
    if (found.isNotEmpty()) {
      return found[0] as Node
    }
  }
  return null
}

internal fun generateDigest(input: File, output: File, algorithm: String) {
  val md = MessageDigest.getInstance(algorithm)
  input.inputStream().use {
    val buffered = it.buffered(8192)
    val buf = ByteArray(8192)
    var rd: Int
    while (true) {
      rd = buffered.read(buf)
      if (rd == -1) break
      md.update(buf, 0, rd)
    }

    output.writeText(
      md.digest().joinToString(separator = "") { eachByte -> "%02x".format(eachByte) } +
        "  ${input.name}"
    )
  }
}

internal fun <T : Any> unsafeCast(o: Any?): T {
  @Suppress("UNCHECKED_CAST") return o as T
}

internal fun <T : Any> parseJson(url: String): T {
  var attempt = 0
  while (true) {
    try {
      return unsafeCast(JsonSlurper().parse(URI(url).toURL())) as T
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

internal fun fetchAsfProjectName(apacheId: String): String {
  val projectsAll: Map<String, Map<String, Any>> =
    parseJson("https://whimsy.apache.org/public/public_ldap_projects.json")
  val projects = unsafeCast<Map<String, Map<String, Any>>>(projectsAll["projects"])
  val project =
    projects[apacheId]
      ?: throw IllegalArgumentException(
        "No project '$apacheId' found in https://whimsy.apache.org/public/public_ldap_projects.json"
      )
  return project["name"] as String
}

internal fun fetchProjectPeople(apacheId: String): ProjectPeople {
  val projectsAll: Map<String, Map<String, Any>> =
    parseJson("https://whimsy.apache.org/public/public_ldap_projects.json")
  val projects = unsafeCast<Map<String, Map<String, Any>>>(projectsAll["projects"])
  val project =
    projects[apacheId]
      ?: throw IllegalArgumentException(
        "No project '$apacheId' found in https://whimsy.apache.org/public/public_ldap_projects.json"
      )
  val isPodlingCurrent = project.containsKey("podling") && project["podling"] == "current"

  val inceptionYear = (project["createTimestamp"] as String).subSequence(0, 4).toString().toInt()

  // Committers
  val peopleProjectRoles: MutableMap<String, MutableList<String>> = mutableMapOf()
  val members = unsafeCast(project["members"]) as List<String>
  members.forEach { member -> peopleProjectRoles.put(member, mutableListOf("Committer")) }

  // (P)PMC Members
  val pmcRoleName = if (isPodlingCurrent) "PPMC member" else "PMC member"
  val owners = unsafeCast(project["owners"]) as List<String>
  owners.forEach { member -> peopleProjectRoles[member]!!.add(pmcRoleName) }

  val projectName: String
  val description: String
  val website: String
  val repository: String
  val licenseUrl: String
  val bugDatabase: String
  if (isPodlingCurrent) {
    val podlingsAll: Map<String, Map<String, Any>> =
      parseJson("https://whimsy.apache.org/public/public_podlings.json")
    val podlings = unsafeCast<Map<String, Map<String, Any>>>(podlingsAll["podling"])
    val podling =
      podlings[apacheId]
        ?: throw IllegalArgumentException(
          "No podling '$apacheId' found in https://whimsy.apache.org/public/public_podlings.json"
        )
    projectName = podling["name"] as String
    description = podling["description"] as String
    val podlingStatus = unsafeCast(podling["podlingStatus"]) as Map<String, Any>
    website = podlingStatus["website"] as String
    // No repository for podlings??
    repository = "https://github.com/apache/$apacheId.git"
    bugDatabase = "https://github.com/apache/$apacheId/issues"
    licenseUrl = "https://www.apache.org/licenses/LICENSE-2.0.txt"

    val champion = podling["champion"] as String
    peopleProjectRoles[champion]!!.add("Champion")

    val mentors = unsafeCast(podling["mentors"]) as List<String>
    mentors.forEach { member -> peopleProjectRoles[member]!!.add("Mentor") }
  } else {
    val tlpPrj: Map<String, Any> =
      parseJson("https://projects.apache.org/json/projects/$apacheId.json")
    website = tlpPrj["homepage"] as String
    repository = (unsafeCast(tlpPrj["repository"]) as List<String>)[0]
    bugDatabase = tlpPrj["bug-database"] as String
    licenseUrl = tlpPrj["license"] as String

    val committeesAll: Map<String, Map<String, Any>> =
      parseJson("https://whimsy.apache.org/public/committee-info.json")
    val committees = unsafeCast<Map<String, Map<String, Any>>>(committeesAll["committees"])
    val committee = unsafeCast<Map<String, Any>>(committees[apacheId])
    val pmcChair = unsafeCast<Map<String, Map<String, Any>>>(committee["chair"])
    projectName = committee["display_name"] as String
    description = committee["description"] as String
    pmcChair.keys.forEach { chair -> peopleProjectRoles[chair]!!.add("PMC Chair") }
  }

  val peopleNames: Map<String, Map<String, Any>> =
    parseJson("https://whimsy.apache.org/public/public_ldap_people.json")
  val people: Map<String, Map<String, Any>> =
    unsafeCast(peopleNames["people"]) as Map<String, Map<String, Any>>
  val peopleList =
    peopleProjectRoles.entries
      .map { entry ->
        val person =
          people[entry.key]
            ?: throw IllegalStateException(
              "No person '${entry.key}' found in https://whimsy.apache.org/public/public_ldap_people.json"
            )
        ProjectMember(entry.key, person["name"]!! as String, entry.value)
      }
      .sortedBy { it.name }

  return ProjectPeople(
    apacheId,
    projectName,
    description,
    website,
    repository,
    licenseUrl,
    bugDatabase,
    inceptionYear,
    peopleList
  )
}

internal class ProjectPeople(
  val apacheId: String,
  val name: String,
  val description: String,
  val website: String,
  val repository: String,
  val licenseUrl: String,
  val bugDatabase: String,
  val inceptionYear: Int,
  val people: List<ProjectMember>
)

internal class ProjectMember(val apacheId: String, val name: String, val roles: List<String>)
