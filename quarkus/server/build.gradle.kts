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
  alias(libs.plugins.quarkus)
  alias(libs.plugins.openapi.generator)
  id("polaris-quarkus")
  id("polaris-license-report")
  id("distribution")
}

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))
  implementation(project(":polaris-service-common"))
  implementation(project(":polaris-quarkus-service"))

  // enforce the Quarkus _platform_ here, to get a consistent and validated set of dependencies
  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-container-image-docker")

  // override dnsjava version in dependencies due to https://github.com/dnsjava/dnsjava/issues/329
  implementation(platform(libs.dnsjava))
}

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", "fast-jar")
  // Pull manifest attributes from the "main" `jar` task to get the
  // release-information into the jars generated by Quarkus.
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

tasks.named("distZip") { dependsOn("quarkusBuild") }

tasks.named("distTar") { dependsOn("quarkusBuild") }

distributions {
  main {
    contents {
      from(project.layout.buildDirectory.dir("quarkus-app"))
      from("../../NOTICE")
      from("../../LICENSE-BINARY-DIST").rename("LICENSE-BINARY-DIST", "LICENSE")
      exclude("lib/main/io.quarkus.quarkus-container-image*")
    }
  }
}
