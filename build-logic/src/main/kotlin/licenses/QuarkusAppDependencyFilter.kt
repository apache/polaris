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

package licenses

import com.github.jk1.license.ConfigurationData
import com.github.jk1.license.ProjectData
import com.github.jk1.license.filter.DependencyFilter
import org.gradle.api.GradleException

/**
 * Restricts the license report to the artifacts that Quarkus actually bundles in the fast-jar
 * output. This avoids documenting pom-only convenience coordinates that appear in the resolved
 * Gradle graph but never ship in the distribution.
 */
class QuarkusAppDependencyFilter : DependencyFilter {

  override fun filter(data: ProjectData?): ProjectData {
    data!!

    val dependenciesFile = data.project.file("build/quarkus-app/quarkus-app-dependencies.txt")
    if (!dependenciesFile.isFile) {
      throw GradleException(
        "Expected Quarkus dependency inventory at '${dependenciesFile.path}'. Run quarkusBuild before generating the license report."
      )
    }

    val bundledDependencies =
      dependenciesFile
        .readLines()
        .asSequence()
        .map { it.trim() }
        .filter { it.isNotEmpty() }
        .mapNotNull { line ->
          val parts = line.split(':')
          if (parts.size >= 5) "${parts[0]}:${parts[1]}" else null
        }
        .toSet()

    val configurations =
      data.configurations
        .map { configuration ->
          ConfigurationData(
            configuration.name,
            configuration.dependencies.filterTo(sortedSetOf()) {
              bundledDependencies.contains("${it.group}:${it.name}")
            },
          )
        }
        .toSortedSet()

    return ProjectData(data.project, configurations, data.importedModules)
  }
}
