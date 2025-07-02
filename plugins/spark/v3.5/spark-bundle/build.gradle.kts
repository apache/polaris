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
  id("com.gradleup.shadow")
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

dependencies { implementation(project(":polaris-spark-${sparkMajorVersion}_${scalaVersion}")) }

tasks.named<ShadowJar>("shadowJar") {
  archiveClassifier = null
  isZip64 = true

  // pack all the dependencies into an uber jar
  configurations = listOf(project.configurations.runtimeClasspath.get())

  // recursively remove all LICENSE and NOTICE file under META-INF, includes
  // directories contains 'license' in the name
  exclude("META-INF/**/*LICENSE*")
  exclude("META-INF/**/*NOTICE*")
  // exclude the top level LICENSE, LICENSE-*.txt and NOTICE
  exclude("LICENSE*")
  exclude("NOTICE*")

  // add polaris customized LICENSE and NOTICE at top level. Note that the
  // customized LICENSE and NOTICE file are called CUSTOM-LICENSE and CUSTOM-NOTICE,
  // and renamed to LICENSE and NOTICE after include, this is to avoid the file
  // being excluded due to the exclude pattern matching used above.
  from("${projectDir}/BUNDLE-LICENSE") { rename { "LICENSE" } }
  from("${projectDir}/BUNDLE-NOTICE") { rename { "NOTICE" } }
}

// ensure the shadow jar job (which will automatically run license addition) is run for both
// `assemble` and `build` task
tasks.named("assemble") { dependsOn("shadowJar") }

tasks.named("build") { dependsOn("shadowJar") }

tasks.named<Jar>("jar") { enabled = false }
