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

import java.time.Duration
import javax.inject.Inject
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.provider.Provider

/**
 * Configuration for running a Polaris server process for Gradle test tasks.
 *
 * The plugin starts the configured server artifact before a decorated
 * [Test][org.gradle.api.tasks.testing.Test] task executes and stops it when the task's root test
 * suite completes. The discovered Quarkus HTTP and management ports are passed to the test JVM
 * using the property names configured here.
 */
abstract class PolarisServerTestRunnerExtension
@Inject
constructor(project: Project, internal val service: Provider<PolarisServerTestService>) {
  /** Server artifact to execute. This must resolve to exactly one runnable `java -jar` artifact. */
  val server: ConfigurableFileCollection = project.objects.fileCollection()

  /**
   * Classpath used to load the optional startup action in an isolated child classloader.
   *
   * The startup action can prepare external services before the Polaris server starts and can add
   * dynamic server system properties or environment variables through the startup context.
   */
  val startupActionClasspath: ConfigurableFileCollection = project.objects.fileCollection()

  /**
   * Fully qualified implementation class name of the optional startup action.
   *
   * The class must implement [org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction]
   * and have a public no-argument constructor.
   */
  val startupActionClass: Property<String> = project.objects.property(String::class.java)

  /** String parameters passed to the optional startup action. */
  val startupActionParameters: MapProperty<String, String> =
    project.objects.mapProperty(String::class.java, String::class.java)

  /** Working directory for the server process. Defaults to `build/polaris-server`. */
  val workingDirectory: DirectoryProperty =
    project.objects
      .directoryProperty()
      .convention(project.layout.buildDirectory.dir("polaris-server"))

  /** Maximum time to wait for the server process to print its Quarkus listen ports. */
  val startupTimeout: Property<Duration> =
    project.objects.property(Duration::class.java).convention(Duration.ofSeconds(30))

  /** Maximum time to wait for the server process to stop gracefully before it is killed. */
  val stopTimeout: Property<Duration> =
    project.objects.property(Duration::class.java).convention(Duration.ofSeconds(15))

  /** Test JVM system property name that receives the discovered HTTP port. */
  val httpPortProperty: Property<String> =
    project.objects.property(String::class.java).convention("quarkus.http.test-port")

  /** Test JVM system property name that receives the discovered management port, when present. */
  val managementPortProperty: Property<String> =
    project.objects.property(String::class.java).convention("quarkus.management.test-port")

  /**
   * System properties passed to the server JVM.
   *
   * These properties are also declared as inputs of the decorated test task.
   */
  val systemProperties: MapProperty<String, String> =
    project.objects.mapProperty(String::class.java, String::class.java)

  /**
   * Environment variables passed to the server process.
   *
   * These variables are also declared as inputs of the decorated test task.
   */
  val environment: MapProperty<String, String> =
    project.objects.mapProperty(String::class.java, String::class.java)

  /**
   * JVM arguments passed before `-jar` when starting the server.
   *
   * These arguments are also declared as inputs of the decorated test task.
   */
  val jvmArguments: ListProperty<String> =
    project.objects.listProperty(String::class.java).convention(emptyList())

  /**
   * Application arguments passed after the server jar path.
   *
   * These arguments are also declared as inputs of the decorated test task.
   */
  val arguments: ListProperty<String> =
    project.objects.listProperty(String::class.java).convention(emptyList())
}

internal fun PolarisServerTestRunnerExtension.conventionFrom(
  other: PolarisServerTestRunnerExtension
) {
  server.setFrom(other.server)
  startupActionClasspath.setFrom(other.startupActionClasspath)
  startupActionClass.convention(other.startupActionClass)
  startupActionParameters.convention(other.startupActionParameters)
  workingDirectory.convention(other.workingDirectory)
  startupTimeout.convention(other.startupTimeout)
  stopTimeout.convention(other.stopTimeout)
  httpPortProperty.convention(other.httpPortProperty)
  managementPortProperty.convention(other.managementPortProperty)
  systemProperties.convention(other.systemProperties)
  environment.convention(other.environment)
  jvmArguments.convention(other.jvmArguments)
  arguments.convention(other.arguments)
}
