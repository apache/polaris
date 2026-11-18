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
  id("polaris-server")
  id("polaris-server-test-runner")
  id("org.kordamp.gradle.jandex")
}

dependencies {
  implementation(project(":polaris-core"))
  // Signs the temporary credentials as HS256 JWTs. polaris-core does not export it.
  implementation(libs.auth0.jwt)
  implementation(libs.slf4j.api)
  // @Identifier on the credential vending mechanism bean.
  implementation(libs.smallrye.common.annotation)

  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.smallrye.config.core)
  // @ConfigDocs annotations for the documentation of the named token map.
  compileOnly(project(":polaris-config-docs-annotations"))
  // Jackson annotations on polaris-core types such as PolarisStorageConfigurationInfo.
  // polaris-core does not export jackson-annotations, so javac warns about missing class files.
  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testImplementation(platform(libs.junit.bom))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testImplementation(libs.assertj.core)
  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.core:jackson-annotations")
  // Builds a SmallRyeConfig in the mapping tests, at the version the Quarkus BOM gives the server.
  testImplementation(platform(libs.quarkus.bom))
  testImplementation("io.smallrye.config:smallrye-config-core")

  // Flow tests run the resource layer on TestServices (runtime-service test fixtures).
  // The four API projects are the same set that semantic-models adds for this purpose.
  testImplementation(testFixtures(project(":polaris-runtime-service")))
  testImplementation(project(":polaris-runtime-service"))
  testImplementation(project(":polaris-api-management-model"))
  testImplementation(project(":polaris-api-management-service"))
  testImplementation(project(":polaris-api-iceberg-service"))
  testImplementation(project(":polaris-api-catalog-service"))
  testImplementation(platform(libs.iceberg.bom))
  testImplementation("org.apache.iceberg:iceberg-api")
  testImplementation("org.apache.iceberg:iceberg-core")
  testImplementation(libs.jakarta.ws.rs.api)
}

val intTestJvmVersion = 21

dependencies {
  // The full Polaris server, with this extension assembled in, that the intTest suite runs against.
  polarisServer(project(path = ":polaris-server", configuration = "quarkusRunner"))
}

testing {
  suites {
    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(platform(libs.quarkus.bom))
        // Shared integration-test harness: PolarisIntegrationTestExtension, PolarisClient, and
        // ManagementApi.
        implementation(project(":polaris-tests"))
        implementation(project(":polaris-runtime-test-common"))
        // Catalog, AwsStorageConfigInfo, and the other management API models.
        implementation(project(":polaris-api-management-model"))
        implementation(libs.jakarta.ws.rs.api)
        // Standalone JAX-RS client runtime and JSON provider for PolarisClient. This suite is a
        // plain JVM that talks to an externally spawned server. RESTEasy matches the repo
        // convention of excluding Jersey.
        implementation("org.jboss.resteasy:resteasy-client")
        implementation("org.jboss.resteasy:resteasy-core")
        implementation(platform(libs.jackson.bom))
        implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")
      }
      targets {
        all {
          val buildDir = project.layout.buildDirectory
          testTask.configure {
            jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
            systemProperty("java.security.manager", "allow")
            maxParallelForks = 1

            val buildDirFile = buildDir.get().asFile
            val logsDir = buildDirFile.resolve("logs")

            withPolarisServer(configurations.polarisServer) {
              environment.put("AWS_REGION", "us-west-2")
              environment.putAll(
                mapOf(
                  "POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret",
                  // Dummy parent token. These two values are not secrets. No call in this suite
                  // reaches Cloudflare R2.
                  "POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY" to "test-r2-access-key",
                  "POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY" to "test-r2-secret-key",
                ),
              )
              systemProperties.putAll(
                mapOf(
                  "quarkus.log.file.path" to logsDir.resolve("polaris.log").absolutePath,
                  "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"" to "[\"S3\"]",
                  "polaris.features.\"SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS\"" to
                    "[\"STS\",\"R2_LOCAL_SIGNING\"]",
                  "polaris.readiness.ignore-severe-issues" to "true",
                ),
              )
            }

            // Gradle runs doFirst actions in reverse order. Registered after
            // withPolarisServer, this cleanup runs before the server starts, so the server log
            // survives the run.
            doFirst {
              logsDir.deleteRecursively()
              buildDirFile.resolve("quarkus.log").delete()
            }
          }
        }
      }
    }
  }
}

listOf("intTestCompileClasspath", "intTestRuntimeClasspath").forEach {
  configurations.named(it) {
    attributes.attribute(
      org.gradle.api.attributes.java.TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE,
      intTestJvmVersion,
    )
  }
}

// Live suite against a real Cloudflare R2 bucket. The name contains "manualtest", so the
// polaris-java convention plugin does not add it to `check`. Run it on demand only:
//   ./gradlew --no-daemon :polaris-extensions-storage-cloudflare-r2:cloudManualTest
// Never run it with --info, --debug, or --scan. A run with the CI variable set requests a scan by
// itself, so such a run must pass --no-scan. The parent token reaches the server and the test
// JVM only through the environment of the Gradle process. Never add it to a system property, to
// withPolarisServer, or to a task input.
testing {
  suites {
    @Suppress("UnstableApiUsage")
    register<JvmTestSuite>("cloudManualTest") {
      dependencies {
        implementation(platform(libs.quarkus.bom))
        // PolarisRestCatalogIntegrationBase, PolarisIntegrationTestExtension, PolarisClient, and
        // ExternalPolarisServerManager.
        implementation(project(":polaris-tests"))
        implementation(project(":polaris-runtime-test-common"))
        implementation(project(":polaris-core"))
        implementation(project(":polaris-api-management-model"))
        implementation(libs.jakarta.ws.rs.api)
        // Standalone JAX-RS client runtime and JSON provider for PolarisClient. This is a plain-JVM
        // suite that talks to an externally started server.
        implementation("org.jboss.resteasy:resteasy-client")
        implementation("org.jboss.resteasy:resteasy-core")
        implementation(platform(libs.jackson.bom))
        implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")

        // PolarisRestCatalogIntegrationBase extends Iceberg's CatalogTests and uses RESTCatalog.
        implementation(platform(libs.iceberg.bom))
        implementation("org.apache.iceberg:iceberg-api")
        implementation("org.apache.iceberg:iceberg-core")
        implementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
        implementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")
        implementation(libs.guava)
        implementation(libs.slf4j.api)

        // Direct S3 fixtures: a client with the parent token and a client with a vended credential.
        implementation(platform(libs.awssdk.bom))
        implementation("software.amazon.awssdk:s3")
        implementation("software.amazon.awssdk:auth")
        implementation("software.amazon.awssdk:regions")
        implementation("software.amazon.awssdk:sdk-core")
        // Iceberg 1.12 builds its default S3 HTTP client, http-client.type "apache", on Apache 5.
        // The AWS SDK also picks Apache 5 over Apache 4 for the direct S3 fixtures when both are
        // present, so the suite needs no Apache 4 client.
        runtimeOnly("software.amazon.awssdk:apache5-client")
      }
      targets {
        all {
          val buildDir = project.layout.buildDirectory
          testTask.configure {
            jvmArgs("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
            systemProperty("java.security.manager", "allow")
            maxParallelForks = 1

            // The result depends on the environment and on a remote system. Gradle does not track
            // either, so never skip the task as up to date and never restore its result from a
            // build cache.
            outputs.upToDateWhen { false }
            outputs.doNotCacheIf("The live suite depends on the environment and on R2") { true }

            val buildDirFile = buildDir.get().asFile
            val logsDir = buildDirFile.resolve("logs")

            // The server inherits the environment of this Gradle process, which carries the parent
            // token and the INTEGRATION_TEST_R2_* variables. Only test settings and non-secret
            // values go here, because the runner records them as task inputs and passes system
            // properties on the server command line.
            withPolarisServer(configurations.polarisServer) {
              environment.putAll(
                mapOf("POLARIS_BOOTSTRAP_CREDENTIALS" to "POLARIS,test-admin,test-secret"),
              )
              // Server settings for the shared REST catalog suite: the log file path, the request
              // body limit, the readiness setting, and the feature flags. Persistence and the
              // secrets manager keep the packaged in-memory defaults, and the suite uses the
              // POLARIS realm only.
              systemProperties.putAll(
                mapOf(
                  "quarkus.log.file.path" to logsDir.resolve("polaris.log").absolutePath,
                  "quarkus.http.limits.max-body-size" to "1000000",
                  "polaris.readiness.ignore-severe-issues" to "true",
                  "polaris.features.\"ALLOW_EXTERNAL_CATALOG_CREDENTIAL_VENDING\"" to "false",
                  "polaris.features.\"ALLOW_EXTERNAL_METADATA_FILE_LOCATION\"" to "false",
                  "polaris.features.\"ALLOW_OVERLAPPING_CATALOG_URLS\"" to "true",
                  "polaris.features.\"ALLOW_SPECIFYING_FILE_IO_IMPL\"" to "true",
                  "polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"" to "true",
                  "polaris.features.\"ALLOW_WILDCARD_LOCATION\"" to "true",
                  "polaris.features.\"ENFORCE_PRINCIPAL_CREDENTIAL_ROTATION_REQUIRED_CHECKING\"" to
                    "true",
                  "polaris.features.\"ENABLE_CATALOG_FEDERATION\"" to "true",
                  "polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"" to
                    "[\"FILE\",\"S3\",\"GCS\",\"AZURE\"]",
                  // R2_LOCAL_SIGNING is opt-in, and the list has no implicit member, so STS stays.
                  "polaris.features.\"SUPPORTED_S3_CREDENTIAL_VENDING_MECHANISMS\"" to
                    "[\"STS\",\"R2_LOCAL_SIGNING\"]",
                  // Vend real credentials, with no subscoping bypass.
                  "polaris.features.\"SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION\"" to "false",
                ),
              )
            }

            // Gradle runs doFirst actions in reverse order. Registered after
            // withPolarisServer, this cleanup runs before the server starts, so the server log
            // survives the run.
            doFirst {
              logsDir.deleteRecursively()
              buildDirFile.resolve("quarkus.log").delete()
            }
          }
        }
      }
    }
  }
}

listOf("cloudManualTestCompileClasspath", "cloudManualTestRuntimeClasspath").forEach {
  configurations.named(it) {
    attributes.attribute(
      org.gradle.api.attributes.java.TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE,
      intTestJvmVersion,
    )
  }
}
