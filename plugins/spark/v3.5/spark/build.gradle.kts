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
  testImplementation("org.apache.hudi:hudi-spark3.5-bundle_${scalaVersion}:1.1.0-SNAPSHOT")

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

  // Optimization: Minimize the JAR (remove unused classes from dependencies)
  // The iceberg-spark-runtime plugin is always packaged along with our polaris-spark plugin,
  // therefore excluded from the optimization.
  minimize { exclude(dependency("org.apache.iceberg:iceberg-spark-runtime-*.*")) }

  // Always run the license file addition after this task completes
  finalizedBy("addLicenseFilesToJar")
}

// Post-processing task to add our project's LICENSE and NOTICE files to the jar and remove any
// other LICENSE or NOTICE files that were shaded in.
tasks.register("addLicenseFilesToJar") {
  dependsOn("createPolarisSparkJar")

  doLast {
    val shadowTask = tasks.named("createPolarisSparkJar", ShadowJar::class.java).get()
    val jarFile = shadowTask.archiveFile.get().asFile
    val tempDir =
      File(
        "${project.layout.buildDirectory.get().asFile}/tmp/jar-cleanup-${shadowTask.archiveBaseName.get()}-${shadowTask.archiveClassifier.get()}"
      )
    val projectLicenseFile = File(projectDir, "LICENSE")
    val projectNoticeFile = File(projectDir, "NOTICE")

    // Validate that required license files exist
    if (!projectLicenseFile.exists()) {
      throw GradleException("Project LICENSE file not found at: ${projectLicenseFile.absolutePath}")
    }
    if (!projectNoticeFile.exists()) {
      throw GradleException("Project NOTICE file not found at: ${projectNoticeFile.absolutePath}")
    }

    logger.info("Processing jar: ${jarFile.absolutePath}")
    logger.info("Using temp directory: ${tempDir.absolutePath}")

    // Clean up temp directory
    if (tempDir.exists()) {
      tempDir.deleteRecursively()
    }
    tempDir.mkdirs()

    // Extract the jar
    copy {
      from(zipTree(jarFile))
      into(tempDir)
    }

    fileTree(tempDir)
      .matching {
        include("**/*LICENSE*")
        include("**/*NOTICE*")
      }
      .forEach { file ->
        logger.info("Removing license file: ${file.relativeTo(tempDir)}")
        file.delete()
      }

    // Remove META-INF/licenses directory if it exists
    val licensesDir = File(tempDir, "META-INF/licenses")
    if (licensesDir.exists()) {
      licensesDir.deleteRecursively()
      logger.info("Removed META-INF/licenses directory")
    }

    // Copy our project's license files to root
    copy {
      from(projectLicenseFile)
      into(tempDir)
    }
    logger.info("Added project LICENSE file")

    copy {
      from(projectNoticeFile)
      into(tempDir)
    }
    logger.info("Added project NOTICE file")

    // Delete the original jar
    jarFile.delete()

    // Create new jar with only project LICENSE and NOTICE files
    ant.withGroovyBuilder {
      "jar"("destfile" to jarFile.absolutePath) { "fileset"("dir" to tempDir.absolutePath) }
    }

    logger.info("Recreated jar with only project LICENSE and NOTICE files")

    // Clean up temp directory
    tempDir.deleteRecursively()
  }
}

// ensure the shadow jar job (which will automatically run license addition) is run for both
// `assemble` and `build` task
tasks.named("assemble") { dependsOn("createPolarisSparkJar") }

tasks.named("build") { dependsOn("createPolarisSparkJar") }
