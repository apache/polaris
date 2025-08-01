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
import groovy.util.NodeList
import org.gradle.api.Project
import org.gradle.api.artifacts.ConfigurationVariant
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.attributes.Bundling
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.component.SoftwareComponentFactory
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.publish.maven.MavenPublication

/**
 * "Proper" publication of shadow-jar instead of the "main" jar, with "the right" Gradle's module
 * metadata that refers to the shadow-jar instead of the "main" jar, which is not published by
 * Polaris.
 *
 * Pieces of this function are taken from the `Java(Base)Plugin` and `ShadowExtension`.
 */
internal fun configureShadowPublishing(
  project: Project,
  mavenPublication: MavenPublication,
  softwareComponentFactory: SoftwareComponentFactory,
) =
  project.run {
    fun isPublishable(element: ConfigurationVariant): Boolean {
      for (artifact in element.artifacts) {
        if (JavaBasePlugin.UNPUBLISHABLE_VARIANT_ARTIFACTS.contains(artifact.type)) {
          return false
        }
      }
      return true
    }

    val shadowJar = project.tasks.named("shadowJar")

    val shadowApiElements =
      project.configurations.create("shadowApiElements") {
        isCanBeConsumed = true
        isCanBeResolved = false
        attributes {
          attribute(Usage.USAGE_ATTRIBUTE, project.objects.named(Usage::class.java, Usage.JAVA_API))
          attribute(
            Category.CATEGORY_ATTRIBUTE,
            project.objects.named(Category::class.java, Category.LIBRARY),
          )
          attribute(
            LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            project.objects.named(LibraryElements::class.java, LibraryElements.JAR),
          )
          attribute(
            Bundling.BUNDLING_ATTRIBUTE,
            project.objects.named(Bundling::class.java, Bundling.SHADOWED),
          )
        }
        outgoing.artifact(shadowJar)
      }

    val component = softwareComponentFactory.adhoc("shadow")
    component.addVariantsFromConfiguration(shadowApiElements) {
      if (isPublishable(configurationVariant)) {
        mapToMavenScope("compile")
      } else {
        skip()
      }
    }
    // component.addVariantsFromConfiguration(configurations.getByName("runtimeElements")) {
    component.addVariantsFromConfiguration(
      project.configurations.getByName("shadowRuntimeElements")
    ) {
      if (isPublishable(configurationVariant)) {
        mapToMavenScope("runtime")
      } else {
        skip()
      }
    }
    // Sonatype requires the javadoc and sources jar to be present, but the
    // Shadow extension does not publish those.
    component.addVariantsFromConfiguration(project.configurations.getByName("javadocElements")) {}
    component.addVariantsFromConfiguration(project.configurations.getByName("sourcesElements")) {}
    mavenPublication.from(component)

    // This a replacement to add dependencies to the pom, if necessary. Equivalent to
    // 'shadowExtension.component(mavenPublication)', which we cannot use.

    mavenPublication.pom {
      withXml {
        val node = asNode()
        val depNode = node.get("dependencies")
        val dependenciesNode =
          if ((depNode as NodeList).isNotEmpty()) depNode[0] as Node
          else node.appendNode("dependencies")
        project.configurations.getByName("shadow").allDependencies.forEach {
          if (it is ProjectDependency) {
            val dependencyNode = dependenciesNode.appendNode("dependency")
            dependencyNode.appendNode("groupId", it.group)
            dependencyNode.appendNode("artifactId", it.name)
            dependencyNode.appendNode("version", it.version)
            dependencyNode.appendNode("scope", "runtime")
          }
        }
      }
    }
  }
