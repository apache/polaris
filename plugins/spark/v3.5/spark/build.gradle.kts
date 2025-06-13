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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins { id("polaris-client") }

// get version information
val sparkMajorVersion = "3.5"
val scalaVersion = getAndUseScalaVersionForProject()
val icebergVersion = pluginlibs.versions.iceberg.get()
val spark35Version = pluginlibs.versions.spark35.get()

val scalaLibraryVersion =
  if (scalaVersion == "2.12") {
    pluginlibs.versions.scala212.get()
  } else {
    pluginlibs.versions.scala213.get()
  }

dependencies {
  // TODO: extract a polaris-rest module as a thin layer for
  //  client to depends on.
  implementation(project(":polaris-api-iceberg-service")) {
    // exclude the iceberg dependencies, use the ones pulled
    // by iceberg-core
    exclude("org.apache.iceberg", "*")
    // exclude all cloud and quarkus specific dependencies to avoid
    // running into problems with signature files.
    exclude("com.azure", "*")
    exclude("software.amazon.awssdk", "*")
    exclude("com.google.cloud", "*")
    exclude("io.airlift", "*")
    exclude("io.smallrye", "*")
    exclude("io.smallrye.common", "*")
    exclude("io.swagger", "*")
    exclude("org.apache.commons", "*")
  }
  implementation(project(":polaris-api-catalog-service")) {
    exclude("org.apache.iceberg", "*")
    exclude("com.azure", "*")
    exclude("software.amazon.awssdk", "*")
    exclude("com.google.cloud", "*")
    exclude("io.airlift", "*")
    exclude("io.smallrye", "*")
    exclude("io.smallrye.common", "*")
    exclude("io.swagger", "*")
    exclude("org.apache.commons", "*")
  }
  implementation(project(":polaris-core")) {
    exclude("org.apache.iceberg", "*")
    exclude("com.azure", "*")
    exclude("software.amazon.awssdk", "*")
    exclude("com.google.cloud", "*")
    exclude("io.airlift", "*")
    exclude("io.smallrye", "*")
    exclude("io.smallrye.common", "*")
    exclude("io.swagger", "*")
    exclude("org.apache.commons", "*")
  }

  implementation("org.apache.iceberg:iceberg-core:${icebergVersion}")
  compileOnly("org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0")

  implementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
  ) {
    // exclude the iceberg rest dependencies, use the ones pulled
    // with iceberg-core dependency
    exclude("org.apache.iceberg", "iceberg-core")
  }

  compileOnly("org.scala-lang:scala-library:${scalaLibraryVersion}")
  compileOnly("org.scala-lang:scala-reflect:${scalaLibraryVersion}")
  compileOnly("org.apache.spark:spark-sql_${scalaVersion}:${spark35Version}") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(libs.mockito.core)

  testImplementation(
    "org.apache.iceberg:iceberg-spark-runtime-3.5_${scalaVersion}:${icebergVersion}"
  )
  testImplementation("org.apache.spark:spark-sql_${scalaVersion}:${spark35Version}") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }
}

// TODO: replace the check using gradlew checkstyle plugin
tasks.register("checkNoDisallowedImports") {
  doLast {
    // List of disallowed imports. Right now, we disallow usage of shaded or
    // relocated libraries in the iceberg spark runtime jar.
    val disallowedImports =
      listOf("import org.apache.iceberg.shaded.", "org.apache.iceberg.relocated.")

    // Directory to scan for Java files
    val sourceDirs = listOf(file("src/main/java"), file("src/test/java"))

    val violations = mutableListOf<String>()
    // Scan Java files in each directory
    sourceDirs.forEach { sourceDir ->
      fileTree(sourceDir)
        .matching {
          include("**/*.java") // Only include Java files
        }
        .forEach { file ->
          val content = file.readText()
          disallowedImports.forEach { importStatement ->
            if (content.contains(importStatement)) {
              violations.add(
                "Disallowed import found in ${file.relativeTo(projectDir)}: $importStatement"
              )
            }
          }
        }
    }

    if (violations.isNotEmpty()) {
      throw GradleException("Disallowed imports found! $violations")
    }
  }
}

tasks.named("check") { dependsOn("checkNoDisallowedImports") }

tasks.register<ShadowJar>("createPolarisSparkJar") {
  archiveClassifier = "bundle"
  isZip64 = true

  // include the LICENSE and NOTICE files for the shadow Jar
  from(projectDir) {
    include("LICENSE")
    include("NOTICE")
  }

  // pack both the source code and dependencies
  from(sourceSets.main.get().output)
  configurations = listOf(project.configurations.runtimeClasspath.get())

  // Optimization: Minimize the JAR (remove unused classes from dependencies)
  // The iceberg-spark-runtime plugin is always packaged along with our polaris-spark plugin,
  // therefore excluded from the optimization.
  minimize {
    exclude(dependency("org.apache.iceberg:iceberg-spark-runtime-*.*"))
    exclude(dependency("org.apache.iceberg:iceberg-core*.*"))
    exclude(dependency("org.apache.avro:avro*.*"))
  }

  relocate("com.fasterxml", "org.apache.polaris.shaded.com.fasterxml.jackson")
  relocate("org.apache.avro", "org.apache.polaris.shaded.org.apache.avro")
}

tasks.withType(Jar::class).named("sourcesJar") { dependsOn("createPolarisSparkJar") }
