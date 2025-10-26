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
import org.gradle.api.publish.maven.MavenPublication

/**
 * Configures the content of the generated `pom.xml` files.
 *
 * For all projects except the root project, the pom gets the `<license>`, `<name>`,
 * `<description>`, `<parent>` elements and fixes dependencies in `<dependencyManagement>` to be
 * consumable by Maven.
 *
 * The root project generates the parent pom, containing all the necessary elements to pass Sonatype
 * validation. Most of the information is taken from publicly consumable Apache project information
 * from `https://projects.apache.org/json/projects/<project-name>.json`. Changes to the Apache
 * project metadata, including podling information, will break the reproducibility of the build.
 *
 * Developer and contributor elements are intentionally *not* included in the POM. Such information
 * is not considered stable (enough) to satisfy reproducible build requirements. The generated POM
 * must be exactly the same when built by a release manager and by someone else to verify the built
 * artifact(s).
 */
internal fun configurePom(project: Project, mavenPublication: MavenPublication, task: Task) =
  mavenPublication.run {
    pom {
      if (project != project.rootProject) {
        // Add the license to every pom to make it easier for downstream projects to retrieve the
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
            val prj = EffectiveAsfProject.forProject(project)
            val asfProjectId = prj.asfProject.apacheId

            organization {
              name.set("The Apache Software Foundation")
              url.set("https://www.apache.org/")
            }
            licenses {
              license {
                name.set("Apache-2.0") // SPDX identifier
                url.set(prj.asfProject.licenseUrl)
              }
            }
            mailingLists {
              prj.publishingHelperExtension.mailingLists
                .get()
                .map { id -> prj.mailingList(id) }
                .forEach { ml ->
                  mailingList {
                    name.set(ml.name())
                    subscribe.set(ml.subscribe())
                    unsubscribe.set(ml.unsubscribe())
                    post.set(ml.post())
                    archive.set(ml.archive())
                  }
                }
            }

            scm {
              val codeRepoString: String = prj.codeRepoUrl().get()
              connection.set("scm:git:$codeRepoString")
              developerConnection.set("scm:git:$codeRepoString")
              url.set("$codeRepoString/tree/main")
              val version = project.version.toString()
              if (!version.endsWith("-SNAPSHOT")) {
                val tagPrefix: String = prj.tagPrefix().get()
                tag.set("$tagPrefix-$version")
              }
            }
            issueManagement { url.set(prj.issueTracker()) }

            name.set(prj.fullName())
            description.set(prj.description())
            url.set(prj.projectUrl())
            inceptionYear.set(prj.asfProject.inceptionYear.toString())

            developers { developer { url.set("https://$asfProjectId.apache.org/community/") } }
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
            depName,
          )
      }

      if (depResult != null) {
        val req = depResult.requested as ModuleComponentSelector
        dependency.appendNode("version", req.version)
      }
    }
  }
}
