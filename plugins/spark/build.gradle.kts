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

val sparkMajorVersion = "3.5"
val scalaVersion =
  if (System.getProperty("scalaVersion") != null) {
    System.getProperty("scalaVersion")
  } else {
    System.getProperty("defaultScalaVersion")
  }

dependencies {
  implementation(project(":polaris-api-iceberg-service")) {
    // exclude the iceberg and jackson dependencies, use the
    // dependencies packed in the iceberg-spark dependency
    exclude("org.apache.iceberg", "*")
    exclude("com.fasterxml.jackson.core", "*")
  }

  implementation(
    "org.apache.iceberg:iceberg-spark-runtime-3.5_${scalaVersion}:${libs.versions.icebergspark.get()}"
  )

  compileOnly("org.apache.spark:spark-sql_${scalaVersion}:${libs.versions.spark35.get()}") {
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
    "org.apache.iceberg:iceberg-spark-runtime-3.5_${scalaVersion}:${libs.versions.icebergspark.get()}"
  )
  testImplementation("org.apache.spark:spark-sql_${scalaVersion}:${libs.versions.spark35.get()}") {
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
    "polaris-iceberg-${libs.versions.icebergspark.get()}-spark-runtime-${sparkMajorVersion}_${scalaVersion}"

  dependencies { exclude("META-INF/**") }

  // pack both the source code and dependencies
  from(sourceSets.main.get().output)
  configurations = listOf(project.configurations.runtimeClasspath.get())

  mergeServiceFiles()

  // Optimization: Minimize the JAR (remove unused classes from dependencies)
  // The iceberg-spark-runtime plugin is always packaged along with our polaris-spark plugin,
  // therefore excluded from the optimization.
  minimize { exclude(dependency("org.apache.iceberg:iceberg-spark-runtime-*.*")) }
}

tasks.withType(Jar::class).named("sourcesJar") { dependsOn("createPolarisSparkJar") }
