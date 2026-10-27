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

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb")
}

testing {
  suites {
    // Floci/Docker-backed tests live in `intTest`, matching the rest of the repo
    // (tools/testcontainers/floci-aws, persistence/nosql dynamodb). Keeping them out of `test`
    // means `:polaris-persistence-dynamodb-atomic:test` does not require a Docker daemon.
    register<JvmTestSuite>("intTest") {
      dependencies {
        implementation(project(":polaris-core"))
        // Exports the testcontainers BOM + `org.testcontainers:testcontainers` and the Floci
        // AWS SDK clients as `api`, so they do not need to be declared again here.
        implementation(project(":polaris-floci-aws-testcontainer"))

        implementation(platform(libs.awssdk.bom))
        implementation("software.amazon.awssdk:dynamodb")
      }
    }
  }
}
