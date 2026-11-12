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

package publishing

import groovy.json.JsonOutput
import java.time.Instant
import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.work.DisableCachingByDefault

/** Observed build context, kept outside release archives because it identifies a particular run. */
@DisableCachingByDefault(because = "Records the current build environment and invocation")
abstract class ReleaseBuildEnvironment : DefaultTask() {
  @get:Input abstract val releaseVersion: Property<String>
  @get:Input abstract val gradleVersion: Property<String>
  @get:Input abstract val gradleJvm: MapProperty<String, String>
  @get:Input abstract val environment: MapProperty<String, String>
  @get:Input abstract val ci: MapProperty<String, String>
  @get:Input abstract val buildFlags: MapProperty<String, String>
  @get:Input abstract val requestedTasks: ListProperty<String>
  @get:Input abstract val configuredToolchains: ListProperty<Map<String, String>>
  @get:OutputFile abstract val outputFile: RegularFileProperty

  init {
    configuredToolchains.convention(emptyList())
    outputs.upToDateWhen { false }
  }

  @TaskAction
  fun generate() {
    val record =
      linkedMapOf(
        "schemaVersion" to 1,
        "scope" to "source-binary-maven",
        "generatedAt" to Instant.now().toString(),
        "release" to linkedMapOf("version" to releaseVersion.get()),
        "gradleVersion" to gradleVersion.get(),
        "gradleJvm" to gradleJvm.get().toSortedMap(),
        "environment" to environment.get().toSortedMap(),
        "ci" to ci.get().toSortedMap(),
        "build" to (buildFlags.get().toSortedMap() + ("requestedTasks" to requestedTasks.get())),
        "configuredToolchains" to
          configuredToolchains.get().distinct().sortedBy { it.getValue("task") },
      )
    val file = outputFile.get().asFile
    file.parentFile.mkdirs()
    file.writeText(JsonOutput.prettyPrint(JsonOutput.toJson(record)) + "\n")
    logger.lifecycle("Release build environment: {}", file)
  }
}
