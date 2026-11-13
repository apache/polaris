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

// Copyright (C) 2026 Dremio

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import licenses.GenerateShadowLicenseInventory
import publishing.GenerateDigest

val shadowJar = providers.provider { tasks.named<ShadowJar>("createPolarisSparkJar").get() }
val shadowSelected = shadowJar.map { it.includedDependencies }
val shadowMinimized = shadowJar.flatMap { it.minimizeJar }
val shadowMetadata =
  shadowJar
    .flatMap { it.configurations }
    .map { configurations ->
      configurations
        .flatMap { it.resolvedConfiguration.resolvedArtifacts }
        .groupBy { it.file.absolutePath }
        .mapValues { (path, values) ->
          val metadata =
            values
              .map { artifact ->
                val id = artifact.moduleVersion.id
                mapOf(
                  "group" to id.group,
                  "name" to id.name,
                  "version" to id.version,
                  "classifier" to artifact.classifier.orEmpty(),
                  "type" to artifact.type,
                )
              }
              .distinct()
          require(metadata.size == 1) { "Ambiguous Shadow input metadata: $path" }
          metadata.single()
        }
    }

tasks.register<GenerateShadowLicenseInventory>("generateLicenseInventory") {
  group = "verification"
  description = "Records actual selected Shadow dependency inputs with resolved Maven coordinates"
  selectedDependencies.from(shadowSelected)
  minimized.set(shadowMinimized)
  coordinatesByPath.set(
    shadowMetadata.map { metadata ->
      metadata.mapValues { (_, artifact) ->
        "${artifact.getValue("group")}:${artifact.getValue("name")}:${artifact.getValue("version")}"
      }
    },
  )
  inventory.set(layout.buildDirectory.file("reports/licensee/shadow-inputs.json"))
}

// Cross-classloader contract with configureDistributionSbom() in the isolated license-check plugin:
// sbomShadow contains Gradle providers only. The plugin registers finalizeDistributionSbom;
// GenerateDigest is only visible on this classpath; its outputs return via sbomShadow["digests"].
extra["sbomShadow"] =
  mapOf(
    "selected" to shadowSelected,
    "archive" to shadowJar.flatMap { it.archiveFile },
    "minimized" to shadowMinimized,
    "metadata" to shadowMetadata,
    "digests" to
      tasks
        .register<GenerateDigest>("digestDistributionSbom") {
          files.from(tasks.named("finalizeDistributionSbom").map { it.outputs.files })
        }
        .map { it.outputFiles },
  )
