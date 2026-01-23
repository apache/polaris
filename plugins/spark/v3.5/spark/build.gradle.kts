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

checkstyle {
  configProperties =
    mapOf(
      "org.checkstyle.google.suppressionfilter.config" to
        project.file("checkstyle_suppressions.xml").absolutePath
    )
}

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
  implementation(project(":polaris-core")) { isTransitive = false }

  implementation(
    "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
  )

  compileOnly("org.scala-lang:scala-library:${scalaLibraryVersion}")
  compileOnly("org.scala-lang:scala-reflect:${scalaLibraryVersion}")
  compileOnly("org.apache.spark:spark-sql_${scalaVersion}:${spark35Version}") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
    exclude("org.slf4j", "jul-to-slf4j")
  }

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

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
  archiveClassifier = "bundle"
  isZip64 = true

  // pack both the source code and dependencies
  from(sourceSets.main.get().output)
  configurations = listOf(project.configurations.runtimeClasspath.get())

  // recursively remove all LICENSE and NOTICE file under META-INF, includes
  // directories contains 'license' in the name
  exclude("META-INF/**/*LICENSE*")
  exclude("META-INF/**/*NOTICE*")
  // exclude the top level LICENSE, LICENSE-*.txt and NOTICE
  exclude("LICENSE*")
  exclude("NOTICE*")

  // add polaris customized LICENSE and NOTICE for the bundle jar at top level. Note that the
  // customized LICENSE and NOTICE file are called BUNDLE-LICENSE and BUNDLE-NOTICE,
  // and renamed to LICENSE and NOTICE after include, this is to avoid the file
  // being excluded due to the exclude pattern matching used above.
  from("${projectDir}/BUNDLE-LICENSE") { rename { "LICENSE" } }
  from("${projectDir}/BUNDLE-NOTICE") { rename { "NOTICE" } }
}

// ensure the shadow jar job (which will automatically run license addition) is run for both
// `assemble` and `build` task
tasks.named("assemble") { dependsOn("createPolarisSparkJar") }

tasks.named("build") { dependsOn("createPolarisSparkJar") }
