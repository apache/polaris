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
import licenses.BundleJarLicenseNoticeValidation
import licenses.BundleLicenseValidation

plugins { id("polaris-client") }

checkstyle {
  configProperties =
    mapOf(
      "org.checkstyle.google.suppressionfilter.config" to
        project.file("checkstyle_suppressions.xml").absolutePath
    )
}

sourceSets {
  main { java { srcDir("../../common/src/main/java") } }
  test { java { srcDir("../../common/src/test/java") } }
}

// get version information
val sparkMajorVersion = "3.5"
val scalaVersion = getAndUseScalaVersionForProject()
val icebergVersion = libs.versions.iceberg.get()
val spark35Version = libs.versions.spark35.get()

val scalaLibraryVersion =
  if (scalaVersion == "2.12") {
    libs.versions.scala212.get()
  } else {
    libs.versions.scala213.get()
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

  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

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
  from(sourceSets.main.map { it.output })
  configurations = provider { listOf(project.configurations.runtimeClasspath.get()) }

  // Includes _all_ duplicates (this is applied files processed by `ShadowJar`).
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
  // This setting applies to the _result_ of the `ShadowJar`.
  failOnDuplicateEntries = true

  // Generally, preserve META-INF/maven/*/*/pom.* files for downstream tools that
  // can analyze dependency jars.
  //
  // There are quite a few _duplicated_ occurrences of failureaccess, guava,
  // listenablefuture, error_prone_annotations, j2objc-annotations, gson.
  // Leave those here so that dependency analyzing tools can pick those up.

  exclude(
    // Recursively remove all LICENSE and NOTICE file under META-INF, includes
    // directories contains 'license' in the name.
    "META-INF/**/*LICENSE*",
    "META-INF/**/*NOTICE*",
    // exclude the top level LICENSE, LICENSE-*.txt and NOTICE
    "LICENSE*",
    "NOTICE*",

    // Exclude Jandex indexes
    "META-INF/jandex.idx",

    // From Hive/Hadoop - exclude those to not confuse people.
    "META-INF/DEPENDENCIES",
  )

  // add polaris customized LICENSE and NOTICE for the bundle jar at top level. Note that the
  // customized LICENSE and NOTICE file are called BUNDLE-LICENSE and BUNDLE-NOTICE,
  // and renamed to LICENSE and NOTICE after include, this is to avoid the file
  // being excluded due to the exclude pattern matching used above.
  from("${projectDir}/BUNDLE-LICENSE") { rename { "LICENSE" } }
  from("${projectDir}/BUNDLE-NOTICE") { rename { "NOTICE" } }
}

val createPolarisSparkJar = tasks.named<ShadowJar>("createPolarisSparkJar")

val checkBundleLicense by
  tasks.registering(BundleLicenseValidation::class) {
    description =
      "Validates direct runtimeClasspath dependencies have " +
        "'* Maven group:artifact IDs:' entries in BUNDLE-LICENSE"
    group = "verification"
    bundleLicenseFile.set(project.file("BUNDLE-LICENSE"))
    bundledArtifacts.set(
      provider {
        configurations
          .getByName("runtimeClasspath")
          .resolvedConfiguration
          .resolvedArtifacts
          .filter { it.moduleVersion.id.group != project.group.toString() }
          .map { "${it.moduleVersion.id.group}:${it.moduleVersion.id.name}" }
          .toSet()
      }
    )
    // The BUNDLE-LICENSE is shared between the _2.12 and _2.13 build variants, so it intentionally
    // contains entries for both Scala variants of each artifact. Allow the cross-variant entry so
    // the superfluous check does not produce a false positive when building for the other variant.
    allowedExtraArtifacts.set(
      provider {
        val otherScalaVersion = if (scalaVersion == "2.12") "2.13" else "2.12"
        configurations
          .getByName("runtimeClasspath")
          .resolvedConfiguration
          .resolvedArtifacts
          .filter { it.moduleVersion.id.group != project.group.toString() }
          .map {
            val baseName =
              it.moduleVersion.id.name.replace("_${scalaVersion}", "_${otherScalaVersion}")
            "${it.moduleVersion.id.group}:${baseName}"
          }
          .toSet()
      }
    )
  }

val checkBundleJarLicenseNotice by
  tasks.registering(BundleJarLicenseNoticeValidation::class) {
    description = "Validates the bundle shadow JAR contains top-level LICENSE and NOTICE entries"
    group = "verification"
    bundleJar.set(createPolarisSparkJar.flatMap { it.archiveFile })
    dependsOn(createPolarisSparkJar)
  }

tasks.named("check") { dependsOn(checkBundleLicense, checkBundleJarLicenseNotice) }

// ensure the shadow jar job (which will automatically run license addition) is run for both
// `assemble` and `build` task
tasks.named("assemble") { dependsOn("createPolarisSparkJar") }

tasks.named("build") { dependsOn("createPolarisSparkJar") }
