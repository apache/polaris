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
  id("org.apache.polaris.apprunner")
}

// Gradle configuration to reference the tarball
val polarisTarball by
  configurations.creating { description = "Used to reference the distribution tarball" }

dependencies { polarisTarball(project(":polaris-quarkus-server", "distributionTar")) }

testing {
  suites {
    val demoTest by registering(JvmTestSuite::class)
  }
}

// Directory where the Polaris tarball is extracted to
val unpackedTarball = project.layout.buildDirectory.dir("polaris-tarball")

// Extracts the Polaris tarball, truncating the path
val polarisUnpackedTarball by
  tasks.registering(Sync::class) {
    inputs.files(polarisTarball)
    destinationDir = unpackedTarball.get().asFile
    from(provider { tarTree(polarisTarball.singleFile) })
    eachFile {
      // truncates the path (removes the first path element)
      relativePath = RelativePath(true, *relativePath.segments.drop(1).toTypedArray())
    }
    includeEmptyDirs = false
  }

val demoTest =
  tasks.named("demoTest") {
    // Dependency to have the extracted tarball
    dependsOn(polarisUnpackedTarball)
  }

polarisQuarkusApp {
  includeTask(demoTest)
  // Reference the quarkus-run.jar in the tarball, apprunner plugin will then run that jar
  executableJar = provider { unpackedTarball.get().file("quarkus-run.jar") }
}
