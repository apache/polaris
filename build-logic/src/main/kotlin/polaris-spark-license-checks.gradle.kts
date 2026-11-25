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

tasks.register<GenerateShadowLicenseInventory>("generateLicenseInventory") {
  val shadowJar = tasks.named<ShadowJar>("createPolarisSparkJar")
  group = "verification"
  description = "Records actual selected Shadow dependency inputs with resolved Maven coordinates"
  selectedDependencies.from(shadowJar.map { it.includedDependencies })
  minimized.set(shadowJar.flatMap { it.minimizeJar })
  coordinatesByPath.set(
    shadowJar
      .flatMap { it.configurations }
      .map { configurations ->
        val artifacts = configurations.flatMap { it.resolvedConfiguration.resolvedArtifacts }
        artifacts
          .groupBy { it.file.absolutePath }
          .mapValues { (path, values) ->
            val coordinates = values.map { it.moduleVersion.id.toString() }.distinct()
            require(coordinates.size == 1) { "Ambiguous Shadow input coordinate: $path" }
            coordinates.single()
          }
      },
  )
  inventory.set(layout.buildDirectory.file("reports/licensee/shadow-inputs.json"))
}
