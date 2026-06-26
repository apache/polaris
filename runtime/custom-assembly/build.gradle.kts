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

import io.quarkus.gradle.tasks.QuarkusBuild
import io.quarkus.gradle.tasks.QuarkusRun

plugins {
  alias(libs.plugins.quarkus)
  id("org.kordamp.gradle.jandex")
  id("polaris-runtime")
}

description = "Apache Polaris Custom Server Assembly"

// ─── Plugin selection via Gradle properties ───────────────────────────────
//
// Pass properties on the Gradle command line to select which plugins to include
// in the custom assembly.
//
// Auth plugins         (default: none)
//   -Pcustom.authPlugins=ranger,opa
//   Supported values  : ranger, opa
//
// Federation plugins   (default: none)
//   -Pcustom.federationPlugins=hadoop,hive,bigquery
//   Supported values  : hadoop, hive, bigquery
//
// Extra Maven deps     (default: none)
//   -Pcustom.mavenDeps=com.mysql:mysql-connector-j:9.0.0,org.example:my-plugin:1.0
//   Any artifact resolvable from Maven Central may be listed here. This is the
//   primary way to add JDBC drivers not bundled with Polaris (e.g. MySQL).
//
// Extra JAR files      (default: none)
//   -Pcustom.extraJars=/absolute/path/to/plugin.jar,/another/path/to/ext.jar
//   Paths must be absolute and point to existing files at build time.

fun csvProp(name: String): List<String> =
  (project.findProperty(name) as? String ?: "")
    .split(",")
    .map { it.trim() }
    .filter { it.isNotEmpty() }

val authPlugins = csvProp("custom.authPlugins")
val federationPlugins = csvProp("custom.federationPlugins")
val mavenDeps = csvProp("custom.mavenDeps")
val extraJarPaths = csvProp("custom.extraJars")

// Validate requested plugin names early to give clear error messages
val knownAuthPlugins = setOf("ranger", "opa")
val knownFederationPlugins = setOf("hadoop", "hive", "bigquery")
val unknownAuth = authPlugins - knownAuthPlugins
val unknownFederation = federationPlugins - knownFederationPlugins
if (unknownAuth.isNotEmpty()) {
  throw GradleException("Unknown auth plugin(s): $unknownAuth. Supported: $knownAuthPlugins")
}
if (unknownFederation.isNotEmpty()) {
  throw GradleException(
    "Unknown federation plugin(s): $unknownFederation. Supported: $knownFederationPlugins"
  )
}

dependencies {
  implementation(project(":polaris-runtime-service"))

  // Default persistence: relational JDBC (supports PostgreSQL, CockroachDB, H2)
  runtimeOnly(project(":polaris-relational-jdbc"))
  runtimeOnly("org.postgresql:postgresql")
  runtimeOnly("io.quarkus:quarkus-jdbc-postgresql")

  // ── Auth plugins ──────────────────────────────────────────────────────────
  if ("ranger" in authPlugins) {
    runtimeOnly(project(":polaris-extensions-auth-ranger"))
  }
  if ("opa" in authPlugins) {
    runtimeOnly(project(":polaris-extensions-auth-opa"))
  }

  // ── Federation plugins ────────────────────────────────────────────────────
  if ("hadoop" in federationPlugins) {
    runtimeOnly(project(":polaris-extensions-federation-hadoop"))
  }
  if ("hive" in federationPlugins) {
    runtimeOnly(project(":polaris-extensions-federation-hive"))
  }
  if ("bigquery" in federationPlugins) {
    runtimeOnly(project(":polaris-extensions-federation-bigquery"))
  }

  // ── Extra Maven dependencies (e.g. additional JDBC drivers) ──────────────
  mavenDeps.forEach { coord -> runtimeOnly(coord) }

  // ── Extra JAR files (e.g. third-party or proprietary plugins) ────────────
  extraJarPaths.forEach { path ->
    val jar = file(path)
    if (!jar.exists()) {
      throw GradleException("Extra JAR not found: $path")
    }
    runtimeOnly(files(jar))
  }

  // Enforce the Quarkus platform for a consistent, validated dependency set
  implementation(enforcedPlatform(libs.quarkus.bom)) {
    exclude(group = "com.google.protobuf", module = "protobuf-java")
    exclude(group = "com.google.protobuf", module = "protobuf-java-util")
  }
  implementation("io.quarkus:quarkus-container-image-docker")
}

quarkus {
  quarkusBuildProperties.put("quarkus.package.jar.type", "fast-jar")
  quarkusBuildProperties.putAll(
    provider {
      tasks
        .named("jar", Jar::class.java)
        .get()
        .manifest
        .attributes
        .map { e -> "quarkus.package.jar.manifest.attributes.\"${e.key}\"" to e.value.toString() }
        .toMap()
    }
  )
}

tasks.register("run") {
  group = "application"
  description = "Runs the custom Apache Polaris server assembly"
  dependsOn("quarkusRun")
}

tasks.named<QuarkusRun>("quarkusRun") {
  jvmArgs =
    listOf(
      "-Dpolaris.bootstrap.credentials=POLARIS,root,s3cr3t",
      "-Dquarkus.console.color=true",
      "-Dpolaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"=true",
      "-Dpolaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"=[\"FILE\",\"S3\",\"GCS\",\"AZURE\"]",
      "-Dpolaris.readiness.ignore-severe-issues=true",
      "-Dpolaris.features.\"DROP_WITH_PURGE_ENABLED\"=true",
    )
}

val quarkusBuild = tasks.named<QuarkusBuild>("quarkusBuild")

// Expose the Quarkus application directory so downstream projects can consume it
val distributionElements by
  configurations.creating {
    isCanBeConsumed = true
    isCanBeResolved = false
  }

artifacts {
  add("distributionElements", layout.buildDirectory.dir("quarkus-app")) {
    builtBy("quarkusBuild")
  }
}

// Convenience task: package the custom assembly as a tar.gz alongside the launch script
tasks.register<Tar>("packageCustomAssembly") {
  group = "distribution"
  description =
    "Packages the custom assembly into a tar.gz that is ready to run (server/ + bin/server)"
  dependsOn(quarkusBuild)
  archiveBaseName.set("polaris-custom")
  archiveExtension.set("tar.gz")
  compression = Compression.GZIP
  destinationDirectory.set(layout.buildDirectory.dir("distributions"))

  into("server") {
    from(layout.buildDirectory.dir("quarkus-app")) { exclude("quarkus-app-dependencies.txt") }
  }
  into("bin") { from(rootProject.file("runtime/distribution/bin/server")) }

  doLast {
    val out = archiveFile.get().asFile
    println("\nCustom assembly packaged at:\n  ${out.absolutePath}")
    println("Extract and run with:")
    println("  tar -xzf ${out.name} && ./bin/server")
  }
}

// Print the path to the built quarkus-app after a successful quarkusBuild
tasks.named("quarkusBuild") {
  doLast {
    val appDir = layout.buildDirectory.dir("quarkus-app").get().asFile
    println("\nCustom assembly built successfully.")
    println("  Location : ${appDir.absolutePath}")
    println("  Run with : java -jar ${appDir.absolutePath}/quarkus-run.jar")
    if (authPlugins.isNotEmpty()) println("  Auth     : ${authPlugins.joinToString()}")
    if (federationPlugins.isNotEmpty())
      println("  Federation: ${federationPlugins.joinToString()}")
    if (mavenDeps.isNotEmpty()) println("  Extra deps: ${mavenDeps.joinToString()}")
    if (extraJarPaths.isNotEmpty()) println("  Extra JARs: ${extraJarPaths.joinToString()}")
  }
}
