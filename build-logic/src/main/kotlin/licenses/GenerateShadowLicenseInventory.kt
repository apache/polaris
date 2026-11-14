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

package licenses

import groovy.json.JsonOutput
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault(because = "Small local inventory uses exact resolved artifact paths")
abstract class GenerateShadowLicenseInventory : DefaultTask() {
  @get:InputFiles
  @get:PathSensitive(PathSensitivity.RELATIVE)
  abstract val selectedDependencies: ConfigurableFileCollection

  @get:Input abstract val coordinatesByPath: MapProperty<String, String>

  @get:Input abstract val minimized: Property<Boolean>

  @get:OutputFile abstract val inventory: RegularFileProperty

  @TaskAction
  fun generate() {
    require(!minimized.get()) {
      "Minimized Shadow packaging requires a reviewed inventory strategy"
    }
    val files = selectedDependencies.files
    require(files.isNotEmpty()) { "Expected nonempty Shadow dependency inputs" }
    val coordinates = coordinatesByPath.get()
    val components =
      files
        .map { file ->
          requireNotNull(coordinates[file.absolutePath]) {
            "Shadow dependency input has no resolved Maven coordinate: $file"
          }
        }
        .toSortedSet()
        .map { coordinate ->
          val parts = coordinate.split(':')
          require(parts.size == 3 && parts.all { it.isNotBlank() }) {
            "Expected Maven group, artifact and version: $coordinate"
          }
          val (group, name, version) = parts
          mapOf(
            "group" to group,
            "name" to name,
            "version" to version,
            "purl" to "pkg:maven/$group/$name@$version",
          )
        }
    inventory.get().asFile.apply {
      parentFile.mkdirs()
      writeText(
        JsonOutput.prettyPrint(
          JsonOutput.toJson(
            mapOf("inventoryKind" to "shadow-dependency-inputs", "components" to components),
          ),
        ) + "\n",
      )
    }
  }
}
