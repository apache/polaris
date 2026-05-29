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

plugins {
  id("polaris-server")
  id("org.kordamp.gradle.jandex")
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(libs.slf4j.api)
  implementation(libs.guava)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.fasterxml.jackson.core:jackson-core")
  compileOnly("com.fasterxml.jackson.core:jackson-databind")
  compileOnly(libs.jspecify)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(platform(libs.opentelemetry.instrumentation.bom.alpha))
  compileOnly("io.opentelemetry:opentelemetry-api")

  implementation(libs.smallrye.common.annotation) // @Identifier
  implementation(libs.postgresql)

  compileOnly(project(":polaris-immutables"))
  annotationProcessor(project(":polaris-immutables", configuration = "processor"))

  testImplementation(libs.mockito.junit.jupiter)
  testImplementation(libs.h2)
  testImplementation(testFixtures(project(":polaris-core")))

  testImplementation(platform(libs.testcontainers.bom))

  testImplementation("org.testcontainers:testcontainers-junit-jupiter")
  testImplementation("org.testcontainers:testcontainers-postgresql")

  testImplementation(project(":polaris-container-spec-helper"))
  testImplementation(project(":polaris-runtime-test-common"))
}

val generatedSchemaVersionTestsDir =
  layout.buildDirectory.dir("generated/sources/schemaVersionTests/java")

val generateSchemaVersionTests by
  tasks.registering {
    val schemaFiles =
      files(
        // schema-v0.sql lives in test resources to cover legacy bootstrap behavior; newer H2
        // schema files live in main resources. Scan both so new schema versions get tests
        // automatically.
        fileTree("src/main/resources/h2") { include("schema-v*.sql") },
        fileTree("src/test/resources/h2") { include("schema-v*.sql") },
      )
    inputs.files(schemaFiles)
    outputs.dir(generatedSchemaVersionTestsDir)

    doLast {
      val outputDir =
        generatedSchemaVersionTestsDir
          .get()
          .file("org/apache/polaris/persistence/relational/jdbc")
          .asFile
      delete(outputDir)
      outputDir.mkdirs()

      val schemaVersionPattern = Regex("""schema-v(\d+)\.sql""")
      val schemaVersions =
        schemaFiles.files
          .mapNotNull { schemaVersionPattern.matchEntire(it.name)?.groupValues?.get(1)?.toInt() }
          .toSortedSet()

      check(schemaVersions.isNotEmpty()) { "No H2 schema files found for JDBC tests" }

      schemaVersions.forEach { schemaVersion ->
        outputDir
          .resolve(
            "AtomicMetastoreManagerWithJdbcBasePersistenceImplV${schemaVersion}SchemaTest.java"
          )
          .writeText(
            """
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
            package org.apache.polaris.persistence.relational.jdbc;

            public class AtomicMetastoreManagerWithJdbcBasePersistenceImplV${schemaVersion}SchemaTest
                extends AtomicMetastoreManagerWithJdbcBasePersistenceImplTest {

              @Override
              public int schemaVersion() {
                return ${schemaVersion};
              }
            }
            """
              .trimIndent()
              .plus("\n")
          )
      }
    }
  }

sourceSets { test { java.srcDir(generateSchemaVersionTests) } }

tasks.named("compileTestJava") { dependsOn(generateSchemaVersionTests) }
