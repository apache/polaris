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

val quarkusGroupIds =
  listOf(
    "io.quarkus",
    "io.quarkus.arc",
    "io.quarkus.qute",
    "io.quarkus.resteasy.reactive",
    "io.quarkus.vertx.utils",
  )

// You can override the version like this: -PquarkusVersion=99.99.99
val quarkusVersion: String =
  gradle.providers.gradleProperty("quarkusVersion").orElse("999-SNAPSHOT").get()

val quarkusGroupIdsStr = quarkusGroupIds.map { "'$it'" }.joinToString(", ")

logger.warn(
  """
    Configuring for a local Quarkus build using version $quarkusVersion, expected in the local Maven repository!
      - Maven Central will not be used for the group IDs $quarkusGroupIdsStr.
      - Gradle plugin repo will not be used for the group IDs $quarkusGroupIdsStr.
      - A Maven Local repo will be added to handle the group IDs $quarkusGroupIdsStr.
  """
    .trimIndent()
)

/**
 * This is a special Gradle init script to configure the Polaris build to use a local Quarkus build,
 * installed in the local Maven repository (~/.m2/repository).
 *
 * Local Quarkus builds do not emit a platform bom, so this script also "redirects" the Quarkus
 * platform bom to the "non-platform" bom.
 *
 * Invocation works like any Gradle build, but you have to tell Gradle to use this script:
 *
 * ./gradlew --init-script gradle/quarkus-local.init.gradle.kts ...
 *
 * Prerequisite
 * ============
 * 1. Checkout the Quarkus repository
 * 2. Perform a local build: ./mvnw -Dquickly install
 *
 * Background
 * ==========
 *
 * Gradle init scripts are executed before the build script is evaluated, but *AFTER*
 * settings.gradle[.kts]!
 *
 * The actual execution order is:
 * 1. The pluginManagement block in settings.gradle.kts
 * 2. The rest of the settings.gradle.kts, including the dependencyResolutionManagement block, is
 *    evaluated.
 * 3. This init script is evaluated.
 *
 * We have to apply the repository rules for both the pluginManagement and
 * dependencyResolutionManagement.
 *
 * The pluginManagement block *MUST* be "self-contained", i.e., it does not "see" anything outside
 * itself. This is because the pluginManagement block has to be evaluated first, before anything
 * else.
 *
 * It would be nicer to have this part in settings.gradle.kts, but the excludeGroup+includeGroup
 * statements would "confuse" Renovate (it would "blindly" consider those, unconditionally).
 */

// Used to apply the Quarkus tweaks only to the root project and its children,
// but not to the build-logic.
val rootProjectName = "polaris"

settingsEvaluated {
  if (rootProject.name == rootProjectName) {
    val mavenCentralRepoName = "MavenRepo"
    val gradleCentralRepoName = "Gradle Central Plugin Repository"

    fun RepositoryHandler.applyLocalQuarkus() {
      all {
        if (name == mavenCentralRepoName || name == gradleCentralRepoName) {
          content { quarkusGroupIds.forEach { excludeGroup(it) } }
        }
      }
      mavenLocal { content { quarkusGroupIds.forEach { includeGroup(it) } } }
    }

    pluginManagement {
      repositories { applyLocalQuarkus() }
      resolutionStrategy {
        eachPlugin {
          if (requested.id.id == "io.quarkus") {
            useModule("io.quarkus:io.quarkus.gradle.plugin:${quarkusVersion}")
          }
        }
      }
    }
    dependencyResolutionManagement { repositories { applyLocalQuarkus() } }
  }
}

beforeProject {
  if (rootProject.name == rootProjectName) {
    configurations.all {
      resolutionStrategy {
        eachDependency {
          if (quarkusGroupIds.contains(requested.group)) {
            when (requested.name) {
              "quarkus-fs-util" -> {
                // skip! this is not part of the main Quarkus build
              }
              else -> {
                // Nag user, so it's obvious that we're building for a local Quarkus build
                logger.warn("Using $quarkusVersion for ${requested.module}")
                useVersion(quarkusVersion)
              }
            }
          } else if (requested.group == "io.quarkus.platform" && requested.name == "quarkus-bom") {
            // Nag user, so it's obvious that we're building for a local Quarkus build
            logger.warn(
              "Updating ${requested.module} to io.quarkus:${requested.name}:$quarkusVersion"
            )
            useTarget("io.quarkus:${requested.name}:$quarkusVersion")
          }
        }
      }
    }
  }
}
