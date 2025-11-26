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
  id("org.kordamp.gradle.jandex")
  id("polaris-server")
}

description = "Polaris NoSQL persistence correctness tests"

dependencies {
  testFixturesApi(project(":polaris-persistence-nosql-api"))
  testFixturesApi(project(":polaris-persistence-nosql-impl"))
  testFixturesApi(project(":polaris-persistence-nosql-testextension"))

  testFixturesImplementation(project(":polaris-idgen-api"))
  testFixturesImplementation(project(":polaris-idgen-impl"))
  testFixturesImplementation(project(":polaris-idgen-spi"))

  testFixturesApi(platform(libs.jackson.bom))
  testFixturesApi("com.fasterxml.jackson.core:jackson-annotations")
  testFixturesApi("com.fasterxml.jackson.core:jackson-databind")

  testFixturesImplementation(libs.jakarta.annotation.api)
  testFixturesImplementation(libs.jakarta.validation.api)

  testFixturesCompileOnly(project(":polaris-immutables"))
  testFixturesAnnotationProcessor(project(":polaris-immutables", configuration = "processor"))
}

// Map of `:polaris-persistence-*` projects implementing the database specific parts to the list of
// test-backend names to be exercised.
var dbs = mapOf("inmemory" to listOf("InMemory"), "mongodb" to listOf("MongoDb"))

testing {
  suites {
    dbs.forEach { prjDbs ->
      val prj = prjDbs.key
      prjDbs.value.forEach { db ->
        val dbTaskName = db.replace("-", "")
        register<JvmTestSuite>("correctness${dbTaskName}Test") {
          dependencies {
            runtimeOnly(project(":polaris-persistence-nosql-$prj"))
            implementation(testFixtures(project(":polaris-persistence-nosql-$prj")))
          }

          targets.all { testTask.configure { systemProperty("polaris.testBackend.name", db) } }
        }
      }
    }

    // Test suite to manually run the correctness tests against an externally configured database
    register<JvmTestSuite>("correctnessManualTest") {
      dependencies {
        implementation(project(":polaris-persistence-nosql-standalone"))

        // Pass system properties starting with `polaris.` down to the manually executed test(s) so
        // they can setup the backend via
        // `o.a.p.persistence.api.BackendConfigurer.defaultBackendConfigurer` using smallrye-config.
        targets.all {
          testTask.configure {
            System.getProperties()
              .filter { p -> p.key.toString().startsWith("polaris.") }
              .forEach { p -> systemProperty(p.key.toString(), p.value) }
          }
        }
      }
    }
  }
}
