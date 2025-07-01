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

import publishing.GenerateDigest
import publishing.PublishingHelperPlugin

plugins {
    id("distribution")
    id("signing")
}

description = "Apache Polaris Binary Distribution"

apply<PublishingHelperPlugin>()

val adminProject = project(":polaris-admin")
val serverProject = project(":polaris-server")

// Configurations to resolve artifacts from other projects
val adminDistribution by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
}

val serverDistribution by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
}

dependencies {
    adminDistribution(project(":polaris-admin", "distributionElements"))
    serverDistribution(project(":polaris-server", "distributionElements"))
}

distributions {
    main {
        distributionBaseName.set("polaris-bin")
        contents {
            // Copy admin distribution contents
            into("admin") {
                from(adminDistribution) {
                    exclude("quarkus-app-dependencies.txt")
                }
            }

            // Copy server distribution contents
            into("server") {
                from(serverDistribution) {
                    exclude("quarkus-app-dependencies.txt")
                }
            }

            // Copy scripts to bin directory
            into("bin") {
                from("bin/server")
                from("bin/admin")
            }

            from("README.md")
            from("DISCLAIMER")
            from("LICENSE")
            from("NOTICE")
        }
    }
}

val distTar = tasks.named<Tar>("distTar") {
    compression = Compression.GZIP
}

val distZip = tasks.named<Zip>("distZip") {
}

val digestDistTar =
    tasks.register<GenerateDigest>("digestDistTar") {
        description = "Generate the distribution tar digest"
        dependsOn(distTar)
        file.set { distTar.get().archiveFile.get().asFile }
    }

val digestDistZip =
    tasks.register<GenerateDigest>("digestDistZip") {
        description = "Generate the distribution zip digest"
        dependsOn(distZip)
        file.set { distZip.get().archiveFile.get().asFile }
    }

distTar.configure { finalizedBy(digestDistTar) }

distZip.configure { finalizedBy(digestDistZip) }

if (project.hasProperty("release") || project.hasProperty("signArtifacts")) {
    signing {
        sign(distTar.get())
        sign(distZip.get())
    }
}