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

import java.util.Properties

includeBuild("build-logic") { name = "polaris-build-logic" }

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_21)) {
  throw GradleException(
    """

        Build aborted...

        The Apache Polaris build requires Java 21.
        Detected Java version: ${JavaVersion.current()}

        """
  )
}

rootProject.name = "polaris"

val baseVersion = file("version.txt").readText().trim()

fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

fun listFromProperty(props: Properties, key: String): List<String> {
  val prop = props[key]
  return prop.toString().split(',').filter { it.isNotBlank() }.map { it.trim() }
}

fun polarisProject(name: String, directory: File): ProjectDescriptor {
  include(name)
  val prj = project(":${name}")
  prj.name = name
  prj.projectDir = file(directory)
  return prj
}

loadProperties(file("gradle/projects.main.properties")).forEach { name, directory ->
  polarisProject(name as String, file(directory as String))
}

val ideSyncActive =
  System.getProperty("idea.sync.active").toBoolean() ||
    System.getProperty("eclipse.product") != null ||
    gradle.startParameter.taskNames.any { it.startsWith("eclipse") }

// Setup Spark/Scala projects

val projectsWithReusedSourceDirs = mutableSetOf<String>()
val sparkScala = loadProperties(file("integrations/spark-scala.properties"))

val sparkVersions = listFromProperty(sparkScala, "sparkVersions")

for (sparkVersion in sparkVersions) {
  val scalaVersions =
    sparkScala["sparkVersion-${sparkVersion}-scalaVersions"].toString().split(",").map { it.trim() }
  var first = true
  for (scalaVersion in scalaVersions) {
    for (prj in listFromProperty(sparkScala, "sparkProjects")) {
      val artifactIdPrefix = sparkScala["project.$prj.artifact-id-prefix"].toString()
      val artifactId = "$artifactIdPrefix-${sparkVersion}_$scalaVersion"
      if (first) {
        first = false
      } else {
        projectsWithReusedSourceDirs.add(":$artifactId")
      }
      polarisProject(artifactId, file("integrations/$prj/v${sparkVersion}")).buildFileName =
        "../build.gradle.kts"
    }
    if (ideSyncActive) {
      break
    }
  }
}

// Setup Spark/Scala and "pure" Scala projects

val allScalaVersions = listFromProperty(sparkScala, "allScalaVersions")

var first = true

for (scalaVersion in allScalaVersions) {
  for (prj in listFromProperty(sparkScala, "scalaProjects")) {
    val name = sparkScala["project.$prj.artifact-id-prefix"].toString()
    val artifactId = "${name}_$scalaVersion"
    if (!first) {
      projectsWithReusedSourceDirs.add(":$artifactId")
    }
    polarisProject(artifactId, file("integrations/$prj"))
  }
  if (first) {
    first = false
  }
  if (ideSyncActive) {
    break
  }
}

// Plugin, dependency management, etc

pluginManagement {
  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
  }
}

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories {
    mavenCentral()
    gradlePluginPortal()
  }
}

gradle.beforeProject {
  version = baseVersion
  group = "org.apache.polaris"

  // Mark projects that reuse source directories, so those do not re-run spotless, errorprone, etc
  if (projectsWithReusedSourceDirs.contains(this.path)) {
    project.extra["reused-project-dir"] = true
  }
}
