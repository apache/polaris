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

plugins {
  id("polaris-client")
  alias(libs.plugins.jandex)
}

fun getAndUseScalaVersionForProject(): String {
  val sparkScala = project.name.split("-").last().split("_")

  val scalaVersion = sparkScala[1]

  // direct the build to build/<scalaVersion> to avoid potential collision problem
  project.layout.buildDirectory.set(layout.buildDirectory.dir(scalaVersion).get())

  return scalaVersion
}

// get version information
val sparkMajorVersion = "3.5"
val scalaVersion = getAndUseScalaVersionForProject()
val icebergVersion = pluginlibs.versions.iceberg.get()
val spark35Version = pluginlibs.versions.spark35.get()

dependencies {
  implementation(project(":polaris-api-iceberg-service")) {
    // exclude the iceberg dependencies, use the ones pulled
    // by iceberg-core
    exclude("org.apache.iceberg", "*")
  }
  implementation(project(":polaris-api-catalog-service"))
  implementation(project(":polaris-core")) { exclude("org.apache.iceberg", "*") }

  implementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
  ) {
    // exclude the iceberg rest dependencies, use the ones pulled
    // with iceberg-core dependency
    exclude("org.apache.iceberg.rest", "*")
    exclude("org.apache.iceberg.hadoop", "*")
  }

  implementation("org.apache.iceberg:iceberg-core:${icebergVersion}")

  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

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

tasks.register<ShadowJar>("createPolarisSparkJar") {
  archiveClassifier = null
  archiveBaseName =
    "polaris-iceberg-${icebergVersion}-spark-runtime-${sparkMajorVersion}_${scalaVersion}"
  isZip64 = true

  dependencies { exclude("META-INF/**") }

  // pack both the source code and dependencies
  from(sourceSets.main.get().output)
  configurations = listOf(project.configurations.runtimeClasspath.get())

  mergeServiceFiles()

  // Optimization: Minimize the JAR (remove unused classes from dependencies)
  // The iceberg-spark-runtime plugin is always packaged along with our polaris-spark plugin,
  // therefore excluded from the optimization.
  // minimize { exclude(dependency("org.apache.iceberg:iceberg-spark-runtime-*.*")) }
}

tasks.withType(Jar::class).named("sourcesJar") { dependsOn("createPolarisSparkJar") }
