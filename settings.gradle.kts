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

plugins {
  id("com.gradle.develocity") version "4.1.1"
  id("com.gradle.common-custom-user-data-gradle-plugin") version "2.4.0"
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

val isCI = System.getenv("CI") != null

develocity {
  val isApachePolarisGitHub = "apache/polaris" == System.getenv("GITHUB_REPOSITORY")
  val gitHubRef: String? = System.getenv("GITHUB_REF")
  val isGitHubBranchOrTag =
    gitHubRef != null && (gitHubRef.startsWith("refs/heads/") || gitHubRef.startsWith("refs/tags/"))
  if (isApachePolarisGitHub && isGitHubBranchOrTag) {
    // Use the ASF's Develocity instance when running against the Apache Polaris repository against
    // a branch or tag.
    // This is for CI runs that have access to the secret for the ASF's Develocity instance.
    server = "https://develocity.apache.org"
    projectId = "polaris"
    buildScan {
      uploadInBackground = !isCI
      publishing.onlyIf { it.isAuthenticated }
      obfuscation { ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } } }
    }
  } else {
    // In all other cases, especially PR CI runs, use Gradle's public Develocity instance.
    var cfgPrjId: String? = System.getenv("DEVELOCITY_PROJECT_ID")
    projectId = if (cfgPrjId == null || cfgPrjId.isEmpty()) "polaris" else cfgPrjId
    buildScan {
      val isGradleTosAccepted = "true" == System.getenv("GRADLE_TOS_ACCEPTED")
      val isGitHubPullRequest = gitHubRef?.startsWith("refs/pull/") ?: false
      if (isGradleTosAccepted || (isCI && isGitHubPullRequest && isApachePolarisGitHub)) {
        // Leave TOS agreement to the user, if not running in CI.
        termsOfUseUrl = "https://gradle.com/terms-of-service"
        termsOfUseAgree = "yes"
      }
      System.getenv("DEVELOCITY_SERVER")?.run {
        if (isNotEmpty()) {
          server = this
        }
      }
      if (isGitHubPullRequest) {
        System.getenv("GITHUB_SERVER_URL")?.run {
          val ghUrl = this
          val ghRepo = System.getenv("GITHUB_REPOSITORY")
          val prNumber = gitHubRef!!.substringAfter("refs/pull/").substringBefore("/merge")
          link("GitHub pull request", "$ghUrl/$ghRepo/pull/$prNumber")
        }
      }
      uploadInBackground = !isCI
      publishing.onlyIf { isCI || gradle.startParameter.isBuildScan }
      obfuscation { ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } } }
    }
  }
}
