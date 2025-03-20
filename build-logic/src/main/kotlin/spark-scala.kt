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

import java.lang.IllegalArgumentException
import org.gradle.api.Project
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.api.artifacts.ModuleDependency
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.exclude

fun Project.prepareSparkScalaProject(): SparkScalaVersions {
  val versions = sparkScalaVersionsForProject()

  forceJavaVersionForTests(versions.runtimeJavaVersion)
  addSparkJvmOptions()
  return versions
}

fun Project.prepareScalaProject(): SparkScalaVersions {
  return sparkScalaVersionsForProject()
}

/** Resolves the Spark and Scala major/minor versions from the Gradle project name. */
fun Project.sparkScalaVersionsForProject(): SparkScalaVersions {
  val matchScala = ".*_(\\d+[.]\\d+)".toRegex().matchEntire(project.name)!!
  val matchSpark = ".*-(\\d+[.]\\d+)_\\d+[.]\\d+".toRegex().matchEntire(project.name)

  val sparkMajorMinorVersion = if (matchSpark != null) matchSpark.groups[1]!!.value else ""
  val scalaMajorMinorVersion = matchScala.groups[1]!!.value

  project.layout.buildDirectory.set(layout.buildDirectory.dir(scalaMajorMinorVersion).get())

  return SparkScalaVersions(
    sparkMajorMinorVersion,
    scalaMajorMinorVersion,
    if (sparkMajorMinorVersion.isNotBlank())
      resolveFullSparkVersion(sparkMajorMinorVersion, scalaMajorMinorVersion)
    else "",
    resolveFullScalaVersion(scalaMajorMinorVersion),
    if (sparkMajorMinorVersion.isNotBlank()) javaVersionForSpark(sparkMajorMinorVersion) else 0,
  )
}

/**
 * Apply the `sparkFullVersion` as a `strictly` version constraint and apply [withSparkExcludes] on
 * the current [Dependency].
 */
fun ModuleDependency.sparkDependencyAndExcludes(sparkScala: SparkScalaVersions): ModuleDependency {
  val dep = this as ExternalModuleDependency
  dep.version { strictly(sparkScala.sparkFullVersion) }
  return this.withSparkExcludes()
}

/** Apply a bunch of common dependency-exclusion to the current Spark [Dependency]. */
fun ModuleDependency.withSparkExcludes(): ModuleDependency {
  return this.exclude("commons-logging", "commons-logging")
    .exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("org.slf4j", "slf4j-reload4j")
    .exclude("org.eclipse.jetty", "jetty-util")
    .exclude("org.apache.avro", "avro")
    .exclude("org.apache.arrow", "arrow-vector")
    .exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
}

/**
 * Resolve the full Spark version for the given major/minor Spark + Scala versions from the
 * `spark*-sql-scala*` dependency.
 */
fun Project.resolveFullSparkVersion(
  sparkMajorMinorVersion: String,
  scalaMajorMinorVersion: String,
): String {
  val dotRegex = "[.]".toRegex()
  val sparkVerTrimmed = sparkMajorMinorVersion.replace(dotRegex, "")
  val scalaVerTrimmed = scalaMajorMinorVersion.replace(dotRegex, "")
  return requiredDependencyPreferredVersion("spark$sparkVerTrimmed-sql-scala$scalaVerTrimmed")
}

/**
 * Resolve the full Scala version for the given major/minor Scala version from the
 * `scala*-lang-library` dependency.
 */
fun Project.resolveFullScalaVersion(scalaMajorMinorVersion: String): String {
  val dotRegex = "[.]".toRegex()
  val scalaVerTrimmed = scalaMajorMinorVersion.replace(dotRegex, "")
  return requiredDependencyPreferredVersion("scala$scalaVerTrimmed-lang-library")
}

/**
 * Get the Java LTS version, that is lower than or equal to the currently running Java version, for
 * a given Spark version to be used in tests.
 */
fun javaVersionForSpark(sparkVersion: String): Int {
  return when (sparkVersion) {
    "3.4",
    "3.5" -> 21
    else ->
      throw IllegalArgumentException("Do not know which Java version Spark $sparkVersion supports")
  }
}

class SparkScalaVersions(
  /** Spark major/minor version. */
  val sparkMajorMinorVersion: String,
  /** Scala major/minor version. */
  val scalaMajorMinorVersion: String,
  /** Full Spark version, including the patch version, for dependencies. */
  val sparkFullVersion: String,
  /** Full Scala version, including the patch version, for dependencies. */
  val scalaFullVersion: String,
  /** Java runtime version to be used in tests. */
  val runtimeJavaVersion: Int,
)

/**
 * Adds the JPMS options required for Spark to run on Java 17, taken from the
 * `DEFAULT_MODULE_OPTIONS` constant in `org.apache.spark.launcher.JavaModuleOptions`.
 */
fun Project.addSparkJvmOptions() {
  tasks.withType(Test::class.java).configureEach {
    jvmArgs =
      jvmArgs +
        listOf(
          "-XX:+IgnoreUnrecognizedVMOptions",
          "--add-opens=java.base/java.lang=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
          "--add-opens=java.base/java.io=ALL-UNNAMED",
          "--add-opens=java.base/java.net=ALL-UNNAMED",
          "--add-opens=java.base/java.nio=ALL-UNNAMED",
          "--add-opens=java.base/java.util=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
          "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
          "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
          "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
          "-Djdk.reflect.useDirectMethodHandle=false",
          // Required for Java 23+ if Hadoop stuff is used
          "-Djava.security.manager=allow",
        )
  }
}
