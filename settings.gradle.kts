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

gradle.beforeProject { version = baseVersion }

plugins { id("com.gradle.develocity") version ("3.18.2") }

// Use Apache's Gradle Enterprise instance only in CI and only if the access-key is present
// See
// https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=263426153#ProjectOnboardingInstructionsforDevelocity-GitHubActions
// https://docs.gradle.com/develocity/gradle-plugin/current/#via_environment_variable
val hasDevelocityAccessKey =
  System.getenv("DEVELOCITY_ACCESS_KEY") != null &&
    System.getenv("DEVELOCITY_ACCESS_KEY").isNotBlank()
val isCI = System.getenv("CI") != null

if (isCI) {
  logger.lifecycle(
    "Running build in CI ${if (hasDevelocityAccessKey) "with" else "without"} Develocity access"
  )
}

// Publish a build-scan automatically in CI, else only on demand.
develocity {
  if (isCI) {
    if (hasDevelocityAccessKey) {
      // The GE instance for Apache rejects unauthenticated build scan uploads, so we publish those
      // to Gradle's public one. This means, that CI runs without secrets, aka all PR CI runs, get
      // the build scans published at Gradle's infra, while other CI runs publish to Apache's infra.
      server = "https://ge.apache.org"
    }
    buildScan {
      termsOfUseUrl = "https://gradle.com/terms-of-service"
      termsOfUseAgree = "yes"
      // Add some potentially interesting information from the environment
      listOf(
          "GITHUB_ACTION_REPOSITORY",
          "GITHUB_ACTOR",
          "GITHUB_BASE_REF",
          "GITHUB_HEAD_REF",
          "GITHUB_JOB",
          "GITHUB_REF",
          "GITHUB_REPOSITORY",
          "GITHUB_RUN_ID",
          "GITHUB_RUN_NUMBER",
          "GITHUB_SHA",
          "GITHUB_WORKFLOW"
        )
        .forEach { e ->
          val v = System.getenv(e)
          if (v != null) {
            value(e, v)
          }
        }
      val ghUrl = System.getenv("GITHUB_SERVER_URL")
      if (ghUrl != null) {
        val ghRepo = System.getenv("GITHUB_REPOSITORY")
        val ghRunId = System.getenv("GITHUB_RUN_ID")
        link("Summary", "$ghUrl/$ghRepo/actions/runs/$ghRunId")
        link("PRs", "$ghUrl/$ghRepo/pulls")
      }
      obfuscation { ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } } }
    }
  } else {
    val isBuildScan = gradle.startParameter.isBuildScan
    buildScan {
      publishing { onlyIf { isBuildScan } }
      obfuscation { ipAddresses { addresses -> addresses.map { _ -> "0.0.0.0" } } }
    }
  }
}
