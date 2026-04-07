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

// You must specify the Quarkus version like this: -PquarkusVersion=99.99.99
val quarkusVersionProvider: Provider<String> = gradle.providers.gradleProperty("quarkusVersion")

if (!quarkusVersionProvider.isPresent) {
  throw GradleException("Must specify the Quarkus version to use using -PquarkusVersion=a.b.c")
}

val quarkusVersion = quarkusVersionProvider.get()

val quarkusGroupIdsStr = quarkusGroupIds.map { "'$it'" }.joinToString(", ")

logger.warn(
  """
    Configuring for a Quarkus prerelease using version $quarkusVersion!
  """
    .trimIndent()
)

/**
 * This is a special Gradle init script to configure the Polaris build to use a pre-release Quarkus
 * version.
 *
 * Quarkus pre-releases do not have a platform bom published, so this script also "redirects" the
 * Quarkus platform bom to the "non-platform" bom.
 *
 * Invocation works like any Gradle build, but you have to tell Gradle to use this script:
 *
 * ./gradlew --init-script gradle/quarkus-prerelease.init.gradle.kts -PquarkusVersion=a.b.c ...
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
 */

// Used to apply the Quarkus tweaks only to the root project and its children,
// but not to the build-logic.
val rootProjectName = "polaris"

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
