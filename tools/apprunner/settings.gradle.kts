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

includeBuild("apprunner-build-logic") { name = "polaris-apprunner-build-logic" }

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_21)) {
  throw GradleException(
    """

        Build aborted...

        The Apache Polaris build requires Java 21.
        Detected Java version: ${JavaVersion.current()}

        """
  )
}

rootProject.name = "polaris-apprunner"

val baseVersion = file("../../version.txt").readText().trim()

fun addProject(name: String) {
  var fullName = "polaris-apprunner-$name"
  include(fullName)
  val prj = project(":$fullName")
  prj.projectDir = file(name)
}

listOf("common", "gradle-plugin", "maven-plugin").forEach { addProject(it) }

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
  group = "org.apache.polaris.apprunner"
}
