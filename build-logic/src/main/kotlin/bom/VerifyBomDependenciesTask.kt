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

package bom

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.SetProperty
import org.gradle.api.tasks.CacheableTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction

@CacheableTask
abstract class VerifyBomDependenciesTask : DefaultTask() {

  @get:Input abstract val bomDependencyCoordinates: SetProperty<String>

  @get:Input abstract val projectCoordinatesByPath: MapProperty<String, String>

  @get:Input abstract val excludedProjectPaths: SetProperty<String>

  @TaskAction
  fun verifyBomDependencies() {
    val depConstraints = bomDependencyCoordinates.get()
    val nonBomProjects = excludedProjectPaths.get()

    val missingProjectPaths =
      projectCoordinatesByPath
        .get()
        .filter { (path, coordinate) ->
          !depConstraints.contains(coordinate) && !nonBomProjects.contains(path)
        }
        .keys
        .sorted()

    if (missingProjectPaths.isNotEmpty()) {
      logger.error(
        "The dependencies declared in the BOM might be incomplete, the following declarations might be missing:"
      )
      missingProjectPaths.forEach { logger.error("  api(project(\"$it\"))") }

      throw GradleException(
        "The dependencies declared in the BOM might be incomplete, see logged errors above."
      )
    }
  }
}
