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

import org.gradle.kotlin.dsl.invoke
import org.gradle.kotlin.dsl.named
import publishing.PublishingHelperPlugin
import publishing.digestTaskOutputs
import publishing.signTaskOutputs
import sbom.CyclonedxBundleTask

plugins {
  id("distribution")
  id("signing")
  id("polaris-spotless")
  id("polaris-reproducible")
  id("polaris-sbom-bundle")
}

description = "Apache Polaris Binary Distribution"

apply<PublishingHelperPlugin>()

// Configurations to resolve artifacts from other projects
val adminDistribution by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

val adminSbom by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

val serverSbom by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

val serverDistribution by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

dependencies {
  adminDistribution(project(":polaris-admin", "distributionElements"))
  adminSbom(project(":polaris-admin", "cyclonedxDirectBomAll"))

  serverDistribution(project(":polaris-server", "distributionElements"))
  serverSbom(project(":polaris-server", "cyclonedxDirectBomAll"))

  bundleSboms(project(":polaris-admin", "cyclonedxDirectBomJson"))
  bundleSboms(project(":polaris-server", "cyclonedxDirectBomJson"))
}

distributions {
  main {
    distributionBaseName.set("polaris-bin")
    contents {
      // Copy admin distribution contents
      into("admin") {
        from(adminDistribution) { exclude("quarkus-app-dependencies.txt") }
        from(configurations["adminSbom"]) {
          eachFile {
            name =
              name.replace(
                Regex("^bom[.](xml|json)$"),
                "polaris-admin-${project.version}.cyclonedx.$1",
              )
          }
        }
      }

      // Copy server distribution contents
      into("server") {
        from(serverDistribution) { exclude("quarkus-app-dependencies.txt") }
        from(configurations["serverSbom"]) {
          eachFile {
            name =
              name.replace(
                Regex("^bom[.](xml|json)$"),
                "polaris-server-${project.version}.cyclonedx.$1",
              )
          }
        }
      }

      // Copy scripts to bin directory
      into("bin") {
        from("bin/server")
        from("bin/admin")
      }

      from(tasks.named("cyclonedxBundleBom"))

      from("README.md")
      from("LICENSE")
      from("NOTICE")
    }
  }
}

val distTar = tasks.named<Tar>("distTar") { compression = Compression.GZIP }

val distZip = tasks.named<Zip>("distZip") {}

digestTaskOutputs(distTar)

digestTaskOutputs(distZip)

signTaskOutputs(distTar)

signTaskOutputs(distZip)

tasks.named<CyclonedxBundleTask>("cyclonedxBundleBom") {
  val baseName = distributions.main.get().distributionBaseName.get()
  jsonOutput.set(
    project.layout.buildDirectory.file("distributions/$baseName-$version.cyclonedx.json")
  )
  xmlOutput.set(
    project.layout.buildDirectory.file("distributions/$baseName-$version.cyclonedx.xml")
  )
  // Note: the polaris-sbom-bundle build plugin sets up signing.
}
