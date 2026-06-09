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
import org.gradle.api.attributes.java.TargetJvmVersion
import org.gradle.api.plugins.jvm.JvmTestSuite
import org.gradle.language.base.plugins.LifecycleBasePlugin

plugins {
  alias(libs.plugins.quarkus)
  id("org.kordamp.gradle.jandex")
  id("polaris-runtime")
  id("polaris-server-test-runner")
}

val intTestJvmVersion = 21
val sparkStartupAction = sourceSets.create("sparkStartupAction")
val sparkStartupActionCompileOnly by configurations.getting
val sparkStartupActionImplementation by configurations.getting
val quarkusBuild = tasks.named<QuarkusBuild>("quarkusBuild")
val localPolarisServer = files(provider { quarkusBuild.get().fastJar.resolve("quarkus-run.jar") })

localPolarisServer.builtBy(quarkusBuild)

configurations.named(sparkStartupAction.runtimeOnlyConfigurationName) {
  extendsFrom(configurations.named(sparkStartupAction.implementationConfigurationName))
}

dependencies {
  polarisServer(localPolarisServer)

  // must be enforced to get a consistent and validated set of dependencies
  implementation(enforcedPlatform(libs.quarkus.bom)) {
    exclude(group = "org.antlr", module = "antlr4-runtime")
    exclude(group = "org.scala-lang", module = "scala-library")
    exclude(group = "org.scala-lang", module = "scala-reflect")
  }

  implementation(project(":polaris-runtime-service"))
  runtimeOnly(project(":polaris-extensions-federation-hadoop"))
  runtimeOnly(project(":polaris-extensions-federation-hive")) {
    // Brings shaded parquet 1.10 which conflicts with Iceberg's parquet 1.16 in test code.
    exclude("org.apache.parquet", "parquet-hadoop-bundle")
  }

  // Adds hadoop-aws to the Polaris JVM classpath so Hive federation's HiveCatalog can use
  // HadoopFileIO (its default) against an s3a:// warehouse.
  runtimeOnly(libs.hadoop.aws) {
    // Exclude the AWS SDK V2 fat-jar `bundle`: hadoop-aws 3.4.3 pulls bundle:2.35.4, which clashes
    // with Polaris's individual SDK modules. The required modules (s3, sts, etc.) are already on
    // the classpath via the awssdk-bom.
    exclude("software.amazon.awssdk", "bundle")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("log4j", "log4j")
  }
  // hadoop-aws references s3-transfer-manager classes but doesn't declare a direct dependency
  // (the bundle fat-jar provided them); add it explicitly at the BOM-managed version.
  runtimeOnly("software.amazon.awssdk:s3-transfer-manager")

  sparkStartupActionCompileOnly("org.apache.polaris.server-test-runner:polaris-server-test-runner")
  sparkStartupActionImplementation(project(":polaris-rustfs-testcontainer"))
}

@Suppress("UnstableApiUsage")
fun JvmTestSuite.configureSparkIntegrationDependencies() {
  dependencies {
    implementation(project(":polaris-tests"))
    implementation(testFixtures(project(":polaris-runtime-service")))
    implementation(project(":polaris-runtime-test-common"))

    implementation(platform(libs.awssdk.bom))
    implementation("software.amazon.awssdk:glue")
    implementation("software.amazon.awssdk:kms")
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:url-connection-client")

    // Required for Spark integration tests
    implementation(enforcedPlatform(libs.scala212.lang.library))
    implementation(enforcedPlatform(libs.scala212.lang.reflect))
    implementation(libs.javax.servlet.api)
    implementation(libs.antlr4.runtime.spark35)
  }
}

fun Test.configureSparkIntegrationTestTask(
  suiteName: String,
  skipCredentialSubscoping: Boolean,
  storageAccessKey: String? = null,
  storageSecretKey: String? = null,
  withHiveRustfsStartupAction: Boolean = false,
) {
  environment("AWS_REGION", providers.environmentVariable("AWS_REGION").getOrElse("us-west-2"))
  jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
  // Need to allow a java security manager after Java 21, for Subject.getSubject to work
  // "getSubject is supported only if a security manager is allowed".
  systemProperty("java.security.manager", "allow")

  val logsDir = project.layout.buildDirectory.dir("logs/$suiteName")
  val hiveRustfsProperties = project.layout.buildDirectory.file("$suiteName/hive-rustfs.properties")

  doFirst {
    val logsDirFile = logsDir.get().asFile
    logsDirFile.deleteRecursively()
    logsDirFile.mkdirs()
  }

  if (withHiveRustfsStartupAction) {
    systemProperty(
      "polaris.spark-tests.hive-rustfs.properties",
      hiveRustfsProperties.get().asFile.absolutePath,
    )
  }

  withPolarisServer(configurations.polarisServer) {
    if (withHiveRustfsStartupAction) {
      startupActionClasspath.from(sparkStartupAction.runtimeClasspath)
      startupActionClass.set("org.apache.polaris.service.spark.it.SparkTestsStartupAction")
      startupActionParameters.put(
        "hiveRustfsProperties",
        hiveRustfsProperties.get().asFile.absolutePath,
      )
    }

    environment.put("AWS_REGION", providers.environmentVariable("AWS_REGION").orElse("us-west-2"))
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

    systemProperties.putAll(
      mapOf(
        "quarkus.profile" to "it",
        "java.security.manager" to "allow",
        "quarkus.log.file.enabled" to "true",
        "quarkus.log.file.path" to logsDir.get().asFile.resolve("polaris.log").absolutePath,
        "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"" to
          "[\"FILE\",\"S3\",\"GCS\",\"AZURE\"]",
        "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"" to "true",
        "polaris.features.\"DROP_WITH_PURGE_ENABLED\"" to "true",
        "polaris.features.\"ALLOW_OVERLAPPING_CATALOG_URLS\"" to "true",
        "polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"" to
          skipCredentialSubscoping.toString(),
        "polaris.features.\"ENABLE_CATALOG_FEDERATION\"" to "true",
        "polaris.features.\"SUPPORTED_CATALOG_CONNECTION_TYPES\"" to "[\"ICEBERG_REST\",\"HIVE\"]",
        "polaris.features.\"SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES\"" to
          "[\"IMPLICIT\",\"OAUTH\"]",
        "polaris.features.\"ENABLE_SUB_CATALOG_RBAC_FOR_FEDERATED_CATALOGS\"" to "true",
        "polaris.features.\"ALLOW_DROPPING_NON_EMPTY_PASSTHROUGH_FACADE_CATALOG\"" to "true",
        "polaris.features.\"ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING\"" to "true",
        "polaris.features.\"ALLOW_FEDERATED_CATALOGS_CREDENTIAL_VENDING\"" to "true",
      )
    )
    storageAccessKey?.let { systemProperties.put("polaris.storage.aws.access-key", it) }
    storageSecretKey?.let { systemProperties.put("polaris.storage.aws.secret-key", it) }
  }
  // For Spark integration tests
  addSparkJvmOptions()
}

@Suppress("UnstableApiUsage")
testing {
  suites {
    register<JvmTestSuite>("sparkIntTest") {
      useJUnitJupiter()
      configureSparkIntegrationDependencies()

      targets.configureEach {
        testTask.configure {
          configureSparkIntegrationTestTask(
            suiteName = "sparkIntTest",
            skipCredentialSubscoping = true,
          )
        }
      }
    }

    register<JvmTestSuite>("catalogFederationIntTest") {
      useJUnitJupiter()
      configureSparkIntegrationDependencies()

      targets.configureEach {
        testTask.configure {
          configureSparkIntegrationTestTask(
            suiteName = "catalogFederationIntTest",
            skipCredentialSubscoping = false,
            storageAccessKey = "test-ak-123-catalog-federation",
            storageSecretKey = "test-sk-123-catalog-federation",
          )
        }
      }
    }

    register<JvmTestSuite>("hiveFederationIntTest") {
      useJUnitJupiter()
      configureSparkIntegrationDependencies()
      dependencies { implementation(project(":polaris-rustfs-testcontainer")) }

      targets.configureEach {
        testTask.configure {
          configureSparkIntegrationTestTask(
            suiteName = "hiveFederationIntTest",
            skipCredentialSubscoping = false,
            storageAccessKey = "test-ak-123-hive-federation",
            storageSecretKey = "test-sk-123-hive-federation",
            withHiveRustfsStartupAction = true,
          )
        }
      }
    }
  }
}

listOf(
    "sparkIntTestCompileClasspath",
    "sparkIntTestRuntimeClasspath",
    "catalogFederationIntTestCompileClasspath",
    "catalogFederationIntTestRuntimeClasspath",
    "hiveFederationIntTestCompileClasspath",
    "hiveFederationIntTestRuntimeClasspath",
    "sparkStartupActionCompileClasspath",
    "sparkStartupActionRuntimeClasspath",
  )
  .forEach {
    configurations.named(it).configure {
      attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, intTestJvmVersion)
    }
  }

tasks.named<Test>("intTest") {
  description = "Runs all Spark integration tests."
  group = LifecycleBasePlugin.VERIFICATION_GROUP
  dependsOn("sparkIntTest", "catalogFederationIntTest", "hiveFederationIntTest")
  testClassesDirs = files()
  classpath = files()
}
