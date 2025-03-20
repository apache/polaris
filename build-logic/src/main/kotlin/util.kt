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

import java.lang.IllegalStateException
import org.gradle.api.JavaVersion
import org.gradle.api.Project
import org.gradle.api.artifacts.MinimalExternalModuleDependency
import org.gradle.api.artifacts.VersionCatalogsExtension
import org.gradle.api.invocation.Gradle
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.jvm.toolchain.JavaToolchainService
import org.gradle.kotlin.dsl.getByType

fun Gradle.ideSyncActive(): Boolean =
  System.getProperty("idea.sync.active").toBoolean() ||
    System.getProperty("eclipse.product") != null ||
    startParameter.taskNames.any { it.startsWith("eclipse") }

fun Project.requiredDependencyVersion(dependencyName: String): String =
  requiredDependency(dependencyName).version!!

fun Project.requiredDependencyPreferredVersion(dependencyName: String): String =
  requiredDependency(dependencyName).versionConstraint.preferredVersion

fun Project.requiredDependency(dependencyName: String): MinimalExternalModuleDependency {
  val versionCatalog = extensions.getByType<VersionCatalogsExtension>().named("libs")
  val dependency =
    versionCatalog.findLibrary(dependencyName).orElseThrow {
      IllegalStateException("No library '$dependencyName' defined in version catalog 'libs'")
    }
  return dependency.get()
}

/** Forces all [Test] tasks to use the given Java version. */
fun Project.forceJavaVersionForTests(requestedJavaVersion: Int) {
  if (requestedJavaVersion > 0) {
    tasks.withType(Test::class.java).configureEach {
      val currentJavaVersion = JavaVersion.current().majorVersion.toInt()
      if (requestedJavaVersion != currentJavaVersion) {
        useJavaVersion(requestedJavaVersion)
      }
    }
  }
}

fun Test.useJavaVersion(requestedJavaVersion: Int) {
  val javaToolchains = project.extensions.findByType(JavaToolchainService::class.java)
  logger.info("Configuring Java $requestedJavaVersion for $path test execution")
  javaLauncher.set(
    javaToolchains!!.launcherFor {
      languageVersion.set(JavaLanguageVersion.of(requestedJavaVersion))
    }
  )
}
