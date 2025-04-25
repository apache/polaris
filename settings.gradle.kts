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

fun polarisProject(name: String, directory: File) {
  include(name)
  val prj = project(":${name}")
  prj.name = name
  prj.projectDir = file(directory)
}

val projects = Properties()

loadProperties(file("gradle/projects.main.properties")).forEach { name, directory ->
  polarisProject(name as String, file(directory as String))
}

val ideaActive = System.getProperty("idea.active").toBoolean()

// load the polaris spark plugin projects
val polarisSparkDir = "plugins/spark"
val sparkScalaVersions = loadProperties(file("${polarisSparkDir}/spark-scala.properties"))
val sparkVersions = sparkScalaVersions["sparkVersions"].toString().split(",").map { it.trim() }

// records the spark projects that maps to the same project dir
val noSourceChecksProjects = mutableSetOf<String>()

for (sparkVersion in sparkVersions) {
  val scalaVersions = sparkScalaVersions["scalaVersions"].toString().split(",").map { it.trim() }
  var first = true
  for (scalaVersion in scalaVersions) {
    val sparkArtifactId = "polaris-spark-${sparkVersion}_${scalaVersion}"
    val sparkIntArtifactId = "polaris-spark-integration-${sparkVersion}_${scalaVersion}"
    polarisProject(
      "polaris-spark-${sparkVersion}_${scalaVersion}",
      file("${polarisSparkDir}/v${sparkVersion}/spark"),
    )
    polarisProject(
      "polaris-spark-integration-${sparkVersion}_${scalaVersion}",
      file("${polarisSparkDir}/v${sparkVersion}/integration"),
    )
    if (first) {
      first = false
    } else {
      noSourceChecksProjects.add(":$sparkArtifactId")
      noSourceChecksProjects.add(":$sparkIntArtifactId")
    }
    // Skip all duplicated spark client projects while using Intelij IDE.
    // This is to avoid problems during dependency analysis and sync when
    // using Intelij, like "Multiple projects in this build have project directory".
    if (ideaActive) {
      break
    }
  }
}

gradle.beforeProject {
  if (noSourceChecksProjects.contains(this.path)) {
    project.extra["duplicated-project-sources"] = true
  }
}

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

dependencyResolutionManagement {
  // version catalog used by the polaris plugin code, such as polaris-spark-3.5
  versionCatalogs { create("pluginlibs") { from(files("plugins/pluginlibs.versions.toml")) } }
}

gradle.beforeProject {
  version = baseVersion
  group = "org.apache.polaris"
}
