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
package org.apache.polaris.server.test.runner

import org.gradle.api.Plugin
import org.gradle.api.Project

class PolarisServerTestRunnerPlugin : Plugin<Project> {
  override fun apply(project: Project) {
    val serverConfiguration =
      project.configurations.create(POLARIS_SERVER_CONFIGURATION) {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
        description = "Runnable Polaris server artifact used by integration test tasks."
      }

    // We need one service per project to prevent classloader isolation issues.
    // Build services are global to the current Gradle build invocation and shared
    // across projects. As plugins are applied to projects, those use a "per project"
    // class loader, which can then cause classic `ClassCastException` with a cause
    // that `PolarisServerTestService$Inject_` from class loader A cannot be cast to
    // `PolarisServerTestService` from class loader B.
    // Using a project-scoped service name prevents this issue.
    val serviceName = "polarisServerTestRunner-${project.path}"
    val service =
      project.gradle.sharedServices.registerIfAbsent(
        serviceName,
        PolarisServerTestService::class.java,
      ) {}

    val extension =
      project.extensions.create(
        POLARIS_SERVER_EXTENSION,
        PolarisServerTestRunnerExtension::class.java,
        project,
        service,
      )
    extension.server.convention(serverConfiguration)
  }

  companion object {
    const val POLARIS_SERVER_CONFIGURATION = "polarisServer"
    const val POLARIS_SERVER_EXTENSION = "polarisServerTestRunner"
  }
}
