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
        // Add the license to every pom to make it easier for downstream project to retrieve the
        // license.
        licenses {
          license {
            name.set("Apache-2.0") // SPDX identifier
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
          mavenPom.run {
            val asfName = e.asfProjectName.get()
            val projectPeople = fetchProjectPeople(asfName)

            organization {
              name.set("The Apache Software Foundation")
              url.set("https://www.apache.org/")
            }
            licenses {
              license {
                name.set("Apache-2.0") // SPDX identifier
                url.set(projectPeople.licenseUrl)
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
              val codeRepo: String = projectPeople.repository
              connection.set("scm:git:$codeRepo")
              developerConnection.set("scm:git:$codeRepo")
              url.set("$codeRepo/tree/main")
              tag.set("main")
            }
            issueManagement { url.set(projectPeople.bugDatabase) }

            name.set(projectPeople.name)
            description.set(projectPeople.description)
            url.set(projectPeople.website)
            inceptionYear.set(projectPeople.inceptionYear.toString())

            developers {
              projectPeople.people.forEach { person ->
                developer {
                  this.id.set(person.apacheId)
                  this.name.set(person.name)
                  this.organization.set("Apache Software Foundation")
                  this.email.set("${person.apacheId}@apache.org")
                  this.roles.addAll(person.roles)
                }
              }
            }

            addContributorsToPom(mavenPom, asfName, "Apache ${projectPeople.name}")
          }
        }
      }
    }
  }

/** Adds contributors as returned by GitHub, in descending `contributions` order. */
fun addContributorsToPom(mavenPom: MavenPom, asfName: String, asfProjectName: String) =
  mavenPom.run {
    contributors {
      val contributors: List<Map<String, Any>> =
        parseJson("https://api.github.com/repos/apache/$asfName/contributors?per_page=1000")
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
