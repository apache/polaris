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

import org.apache.polaris.server.test.runner.PolarisServerTestRunnerExtension
import org.apache.polaris.server.test.runner.PolarisServerTestRunnerPlugin
import org.apache.polaris.server.test.runner.configurePolarisServer
import org.apache.polaris.server.test.runner.conventionFrom
import org.gradle.api.Action
import org.gradle.api.file.FileCollection
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.testing.Test
import org.gradle.process.CommandLineArgumentProvider

/**
 * Runs a Polaris server from [server] for the duration of this [Test] task.
 *
 * The file collection must resolve to exactly one runnable jar. The server is started only when the
 * test task executes, not during configuration. The discovered HTTP and management ports are passed
 * to the test JVM using the default property names from [PolarisServerTestRunnerExtension].
 */
fun Test.withPolarisServer(server: FileCollection) {
  withPolarisServer(server) {}
}

/**
 * Runs a Polaris server from the file collection provided by [server] for the duration of this
 * [Test] task.
 *
 * This overload is convenient for lazily named configurations such as
 * `withPolarisServer(configurations.polarisServer)`.
 */
fun Test.withPolarisServer(server: Provider<out FileCollection>) {
  withPolarisServer(server) {}
}

/**
 * Runs a Polaris server from [server] for the duration of this [Test] task and customizes its
 * process settings using [configure].
 *
 * Use this overload when the server artifact is task-specific, for example
 * `withPolarisServer(configurations.polarisServer) { ... }`. Dynamic port values are passed via a
 * [CommandLineArgumentProvider] so they do not become test task inputs.
 */
fun Test.withPolarisServer(
  server: FileCollection,
  configure: Action<PolarisServerTestRunnerExtension>,
) {
  val extension = polarisServerTestRunnerExtension()
  extension.server.setFrom(server)
  configure.execute(extension)
  configurePolarisServer(extension)
}

/**
 * Runs a Polaris server from the file collection provided by [server] for the duration of this
 * [Test] task and customizes its process settings using [configure].
 *
 * Use this overload when the server artifact is exposed through a lazy Gradle provider, for example
 * `withPolarisServer(configurations.polarisServer) { ... }`.
 */
fun Test.withPolarisServer(
  server: Provider<out FileCollection>,
  configure: Action<PolarisServerTestRunnerExtension>,
) {
  val extension = polarisServerTestRunnerExtension()
  extension.server.setFrom(server)
  configure.execute(extension)
  configurePolarisServer(extension)
}

/**
 * Runs a Polaris server for the duration of this [Test] task using the plugin extension's
 * configured [PolarisServerTestRunnerExtension.server] artifact.
 *
 * Use this overload when the `polarisServerTestRunner` extension is configured globally and the
 * test task only needs to opt into the server lifecycle.
 */
fun Test.withPolarisServer(configure: Action<PolarisServerTestRunnerExtension>) {
  val extension = polarisServerTestRunnerExtension()
  configure.execute(extension)
  configurePolarisServer(extension)
}

private fun Test.polarisServerTestRunnerExtension(): PolarisServerTestRunnerExtension {
  val projectExtension =
    project.extensions.getByName(PolarisServerTestRunnerPlugin.POLARIS_SERVER_EXTENSION)
      as PolarisServerTestRunnerExtension
  return project.objects
    .newInstance(PolarisServerTestRunnerExtension::class.java, project, projectExtension.service)
    .apply { conventionFrom(projectExtension) }
}
