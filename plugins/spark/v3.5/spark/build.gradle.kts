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
import org.gradle.api.attributes.java.TargetJvmVersion
import org.gradle.api.plugins.jvm.JvmTestSuite

plugins {
  id("polaris-client")
  id("polaris-server-test-runner")
}

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
val icebergVersion = libs.versions.iceberg.get()
val spark35Version = libs.versions.spark35.get()

val scalaLibraryVersion =
  if (scalaVersion == "2.12") {
    libs.versions.scala212.get()
  } else {
    libs.versions.scala213.get()
  }
val errorProneAnnotationsVersion = libs.errorprone.get().versionConstraint.requiredVersion
val intTestJvmVersion = 21

dependencies {
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))

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

testing {
  suites {
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(
          "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
        )

        implementation(project(":polaris-api-management-model"))

        implementation(project(":polaris-runtime-test-common"))

        implementation("org.apache.spark:spark-sql_${scalaVersion}:${spark35Version}")

        // Add spark-hive for Hudi integration - provides HiveExternalCatalog that Hudi needs
        runtimeOnly("org.apache.spark:spark-hive_${scalaVersion}:${spark35Version}")
        // Delta and Hudi initialize Log4j classes from the Spark runtime. Keep Log4j Core aligned
        // with the version used by the Spark tests instead of relying on older Hive transitive
        // deps.
        runtimeOnly("org.apache.logging.log4j:log4j-core:2.26.0")

        implementation("io.delta:delta-spark_${scalaVersion}:3.3.1")
        implementation("org.apache.hudi:hudi-spark3.5-bundle_${scalaVersion}:1.1.1")

        // The hudi-spark-bundle includes most Hive libraries but excludes hive-exec to keep size
        // manageable
        // This matches what Spark 3.5 distribution provides (hive-exec-2.3.10-core.jar)
        implementation("org.apache.hive:hive-exec:2.3.10:core")

        implementation(platform(libs.jackson.bom))
        implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")
        implementation(libs.jakarta.ws.rs.api)
        compileOnly("com.google.errorprone:error_prone_annotations:${errorProneAnnotationsVersion}")

        implementation(testFixtures(project(":polaris-runtime-service")))

        implementation(platform(libs.awssdk.bom))
        implementation("software.amazon.awssdk:glue")
        implementation("software.amazon.awssdk:kms")
        implementation("software.amazon.awssdk:dynamodb")

        implementation(platform(libs.testcontainers.bom))
        implementation("org.testcontainers:testcontainers")
        implementation(libs.s3mock.testcontainers)

        // Required for Spark integration tests
        implementation(enforcedPlatform("org.scala-lang:scala-library:${scalaLibraryVersion}"))
        implementation(enforcedPlatform("org.scala-lang:scala-reflect:${scalaLibraryVersion}"))
        implementation(libs.javax.servlet.api)
        implementation(libs.antlr4.runtime.spark35)
      }

      targets.configureEach {
        testTask.configure {
          environment(
            "AWS_REGION",
            providers.environmentVariable("AWS_REGION").getOrElse("us-west-2"),
          )
          jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
          // Need to allow a java security manager after Java 21, for Subject.getSubject to work
          // "getSubject is supported only if a security manager is allowed".
          systemProperty("java.security.manager", "allow")
          val logsDir = project.layout.buildDirectory.get().asFile.resolve("logs")
          // delete files from previous runs
          doFirst {
            // delete log files written by Polaris
            logsDir.deleteRecursively()
          }
          withPolarisServer(configurations.polarisServer) {
            environment.put(
              "AWS_REGION",
              providers.environmentVariable("AWS_REGION").orElse("us-west-2"),
            )
            environment.put(
              "AWS_ACCESS_KEY_ID",
              providers.environmentVariable("AWS_ACCESS_KEY_ID").orElse("ap1"),
            )
            environment.put(
              "AWS_SECRET_ACCESS_KEY",
              providers.environmentVariable("AWS_SECRET_ACCESS_KEY").orElse("s3cr3t"),
            )
            environment.put("AWS_EC2_METADATA_DISABLED", "true")
            environment.put("POLARIS_BOOTSTRAP_CREDENTIALS", "POLARIS,test-admin,test-secret")
            systemProperties.put("quarkus.profile", "it")
            systemProperties.put(
              "quarkus.log.file.path",
              logsDir.resolve("polaris.log").absolutePath,
            )
          }
          // For Spark integration tests
          addSparkJvmOptions()
        }
      }
    }
  }
}

sourceSets {
  main { java { srcDir("../../common/src/main/java") } }
  test { java { srcDir("../../common/src/test/java") } }
  named("intTest") {
    java { srcDir("../../common/src/intTest/java") }
    resources { srcDir("../../common/src/intTest/resources") }
  }
}

listOf("intTestCompileClasspath", "intTestRuntimeClasspath").forEach {
  // :polaris-runtime-test-common and :polaris-runtime-service (testFixtures) require JVM 21
  // compatibility.
  configurations.named(it).configure {
    attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, intTestJvmVersion)
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

// ensure the shadow jar job (which will automatically run license addition) is run for both
// `assemble` and `build` task
tasks.named("assemble") { dependsOn("createPolarisSparkJar") }

tasks.named("build") { dependsOn("createPolarisSparkJar") }
