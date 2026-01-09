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

import publishing.PublishingHelperPlugin
import publishing.digestTaskOutputs
import publishing.signTaskOutputs

plugins {
  id("distribution")
  id("signing")
  id("polaris-spotless")
  id("polaris-reproducible")
}

description = "Apache Polaris Binary Distribution"

apply<PublishingHelperPlugin>()

val serverProject = project(":polaris-server")

// Configuration to resolve artifacts from server project
val serverDistribution by
  configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
  }

dependencies { serverDistribution(project(":polaris-server", "distributionElements")) }

distributions {
  main {
    distributionBaseName.set("polaris-bin")
    contents {
      // Copy server distribution contents (contains both server and admin tool functionality)
      into("server") { from(serverDistribution) { exclude("quarkus-app-dependencies.txt") } }

      // Copy scripts to bin directory
      // Both server and admin scripts now use the unified server JAR
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

val distTar = tasks.named<Tar>("distTar") { compression = Compression.GZIP }

val distZip = tasks.named<Zip>("distZip") {}

digestTaskOutputs(distTar)

digestTaskOutputs(distZip)

signTaskOutputs(distTar)

signTaskOutputs(distZip)
