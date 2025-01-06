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

import groovy.util.Node
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.component.ModuleComponentSelector
import org.gradle.api.publish.maven.MavenPom
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.internal.extensions.stdlib.capitalized

/**
 * Configures the content of the generated `pom.xml` files.
 *
 * For all projects except the root project, the pom gets the `<license>`, `<name>`,
 * `<description>`, `<parent>` elements and fixes dependencies in `<dependencyManagement>` to be
 * consumable by Maven.
 *
 * The root project generates the parent pom, containing all the necessary elements to pass Sonatype
 * validation and some more information like `<developers>` and `<contributors>`. Most of the
 * information is taken from publicly consumable Apache project information from
 * `https://projects.apache.org/json/projects/<project-name>>.json`. `<developers>` contains all
 * (P)PMC members and committers from that project info JSON, ordered by real name. `<contributors>`
 * is taken from GitHub's
 * `https://api.github.com/repos/apache/<project-name>/contributors?per_page=1000` endpoint to give
 * all contributors credit, ordered by number of contributions (as returned by that endpoint).
 */
internal fun configurePom(project: Project, mavenPublication: MavenPublication, task: Task) =
  mavenPublication.run {
    val e = project.extensions.getByType(PublishingHelperExtension::class.java)

    pom {
      if (project != project.rootProject) {
        name.set(e.mavenName.get())
        description.set(project.description)

        // Add the license to every pom to make it easier for downstream project to retrieve the
        // license.
        licenses {
          license {
            name.set("Apache-2.0") // SPDX identifier
            url.set(e.licenseUrl.get())
          }
        }

        withXml {
          val projectNode = asNode()

          val parentNode = projectNode.appendNode("parent")
          val parent = project.parent!!
          parentNode.appendNode("groupId", parent.group)
          parentNode.appendNode("artifactId", parent.name)
          parentNode.appendNode("version", parent.version)

          addMissingMandatoryDependencyVersions(project, projectNode)
        }
      } else {
        val mavenPom = this

        task.doFirst {
          val asfName = e.asfProjectName.get()
          val (asfPrj, fromPodlings) = fetchAsfProject(asfName)

          val asfProjectName = asfPrj["name"] as String

          mavenPom.name.set(asfProjectName)
          mavenPom.description.set(asfPrj["description"] as String)

          inceptionYear.set(
            (asfPrj["created"] ?: asfPrj["started"]!!).toString().replace("(\\d+)-.*", "\\1")
          )
          url.set(asfPrj["homepage"] as String)
          organization {
            name.set("The Apache Software Foundation")
            url.set("https://www.apache.org/")
          }
          licenses {
            license {
              name.set("Apache-2.0") // SPDX identifier
              url.set(e.licenseUrl.get())
            }
          }
          mailingLists {
            e.mailingLists.get().forEach { ml ->
              mailingList {
                name.set("${ml.capitalized()} Mailing List")
                subscribe.set("$ml-subscribe@$asfName.apache.org")
                unsubscribe.set("$ml-ubsubscribe@$asfName.apache.org")
                post.set("$ml@$asfName.apache.org")
                archive.set("https://lists.apache.org/list.html?$ml@$asfName.apache.org")
              }
            }
          }
          scm {
            val codeRepo: String =
              if (asfPrj.contains("repository")) {
                val repos: List<String> = unsafeCast(asfPrj["repository"]) as List<String>
                repos[0]
              } else {
                "https://github.com/apache/$asfName.git"
              }
            connection.set("scm:git:$codeRepo")
            developerConnection.set("scm:git:$codeRepo")
            url.set("$codeRepo/tree/main")
            tag.set("main")
          }
          issueManagement {
            url.set(
              if (asfPrj.contains("repository")) {
                asfPrj["bug-database"] as String
              } else {
                "https://github.com/apache/$asfName/issues"
              }
            )
          }
          addDevelopersToPom(mavenPom, asfName, e, fromPodlings)
          addContributorsToPom(mavenPom, asfName, asfProjectName)
        }
      }
    }
  }

/** Adds contributors as returned by GitHub, in descending `contributions` order. */
fun addContributorsToPom(mavenPom: MavenPom, asfName: String, asfProjectName: String) =
  mavenPom.run {
    contributors {
      val contributors: List<Map<String, Any>>? =
        parseJson("https://api.github.com/repos/apache/$asfName/contributors?per_page=1000")
      if (contributors != null) {
        contributors
          .filter { contributor -> contributor["type"] == "User" }
          .forEach { contributor ->
            contributor {
              name.set(contributor["login"] as String)
              url.set(contributor["url"] as String)
              organization.set("$asfProjectName, GitHub contributors")
              organizationUrl.set("https://github.com/apache/$asfName")
            }
          }
      }
    }
  }

/** Adds Apache (P)PMC members + committers, in `name` order. */
fun addDevelopersToPom(
  mavenPom: MavenPom,
  asfName: String,
  e: PublishingHelperExtension,
  fromPodlings: Boolean
) =
  mavenPom.run {
    developers {
      // Cannot use check the 'groups' for podlings, because the (only) podling's group
      // references the mentors, not the PPMC members/committers. There seems to be no
      // way to automatically fetch the committers + PPMC members for a podling, except
      // maybe
      val people: Map<String, Map<String, Any>> =
        parseJson("https://projects.apache.org/json/foundation/people.json")!!
      val filteredPeople: List<Pair<String, Map<String, Any>>>
      val pmc: (Pair<String, Map<String, Any>>) -> Boolean
      val mentor: (Pair<String, Map<String, Any>>) -> Boolean
      val pmcRole: String
      if (!fromPodlings) {
        val asfNamePmc = "$asfName-pmc"
        filteredPeople =
          people
            .filter { entry ->
              val groups: List<String> = unsafeCast(entry.value["groups"])
              groups.any { it == asfName || it == asfNamePmc }
            }
            .toList()
        pmc = { (_, info) ->
          val groups: List<String> = unsafeCast(info["groups"])
          groups.contains(asfNamePmc)
        }
        pmcRole = "PMC Member"
        mentor = { (_, _) -> false }
      } else {
        val podlingPpmc = e.podlingPpmcAsfIds.get()
        val podlingMentors = e.podlingMentorsAsfIds.get()
        filteredPeople =
          (e.podlingCommitterAsfIds.get() + podlingPpmc + podlingMentors).map { id ->
            val info = people[id]
            if (info == null) {
              throw GradleException("Person with ASF id '%s' not found in people.json".format(id))
            }
            Pair(id, info)
          }
        pmc = { (id, _) -> podlingPpmc.contains(id) || podlingMentors.contains(id) }
        pmcRole = "PPMC Member"
        mentor = { (id, _) -> podlingMentors.contains(id) }
      }

      val sortedPeople = filteredPeople.sortedBy { (id, info) -> "${info["name"] as String}_$id" }

      sortedPeople.forEach { (id, info) ->
        developer {
          this.id.set(id)
          this.name.set(info["name"] as String)
          this.organization.set("Apache Software Foundation")
          this.email.set("$id@apache.org")
          this.roles.add("Committer")
          if (pmc.invoke(Pair(id, info))) {
            this.roles.add(pmcRole)
          }
          if (mentor.invoke(Pair(id, info))) {
            this.roles.add("Mentor")
          }
        }
      }
    }
  }

/**
 * Scans the generated `pom.xml` for `<dependencies>` in `<dependencyManagement>` that do not have a
 * `<version>` and adds one, if possible. Maven kinda requires `<version>` tags there, even if the
 * `<dependency>` without a `<version>` is a bom and that bom's version is available transitively.
 */
fun addMissingMandatoryDependencyVersions(project: Project, projectNode: Node) {
  xmlNode(xmlNode(projectNode, "dependencyManagement"), "dependencies")?.children()?.forEach {
    val dependency = it as Node
    if (xmlNode(dependency, "version") == null) {
      val depGroup = xmlNode(dependency, "groupId")!!.text()
      val depName = xmlNode(dependency, "artifactId")!!.text()

      var depResult =
        findDependency(project.configurations.findByName("runtimeClasspath"), depGroup, depName)
      if (depResult == null) {
        depResult =
          findDependency(
            project.configurations.findByName("testRuntimeClasspath"),
            depGroup,
            depName
          )
      }

      if (depResult != null) {
        val req = depResult.requested as ModuleComponentSelector
        dependency.appendNode("version", req.version)
      }
    }
  }
}
