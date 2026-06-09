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

import org.gradle.api.plugins.jvm.JvmTestSuite

plugins {
  id("polaris-java")
  id("polaris-server-test-runner")
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

dependencies { polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner")) }

testing {
  suites {
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(
          "org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_${scalaVersion}:${icebergVersion}"
        )
        implementation(project(":polaris-spark-${sparkMajorVersion}_${scalaVersion}"))

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

      targets.all {
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
  named("intTest") {
    java { srcDir("../../common/src/intTest/java") }
    resources { srcDir("../../common/src/intTest/resources") }
  }
}
