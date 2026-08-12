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

import java.io.File
import java.net.URLClassLoader
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupAction
import org.apache.polaris.server.test.runner.spi.PolarisServerStartupContext
import org.gradle.api.GradleException
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.TestDescriptor
import org.gradle.api.tasks.testing.TestListener
import org.gradle.api.tasks.testing.TestResult
import org.gradle.process.CommandLineArgumentProvider

internal fun Test.configurePolarisServer(extension: PolarisServerTestRunnerExtension) {
  val dynamicArguments = PolarisServerArguments()
  var startupAction: AutoCloseable? = null
  jvmArgumentProviders.add(dynamicArguments)
  inputs.files(extension.server).withPathSensitivity(PathSensitivity.RELATIVE)
  inputs.files(extension.startupActionClasspath).withPathSensitivity(PathSensitivity.RELATIVE)
  inputs.property("polarisServerStartupActionClass", extension.startupActionClass.orNull ?: "")
  inputs.properties(extension.startupActionParameters.get())
  inputs.properties(extension.systemProperties.get())
  inputs.properties(extension.environment.get())
  inputs.property("polarisServerJvmArguments", extension.jvmArguments.get())
  inputs.property("polarisServerArguments", extension.arguments.get())
  usesService(extension.service)
  addTestListener(
    object : TestListener {
      override fun beforeSuite(suite: TestDescriptor) {}

      override fun beforeTest(testDescriptor: TestDescriptor) {}

      override fun afterTest(testDescriptor: TestDescriptor, result: TestResult) {}

      override fun afterSuite(suite: TestDescriptor, result: TestResult) {
        if (suite.parent == null) {
          extension.service.get().finished(this@configurePolarisServer)
          startupAction?.closeQuietly()
          startupAction = null
        }
      }
    }
  )

  doFirst {
    val files = extension.server.files
    if (files.size != 1) {
      throw GradleException(
        "Expected exactly one Polaris server artifact, but found ${files.size}: $files"
      )
    }
    val jar = files.single()
    val javaExecutable = javaLauncher.get().executablePath.asFile.absolutePath
    val systemProperties = extension.systemProperties.get().toMutableMap()
    val environment = extension.environment.get().toMutableMap()
    try {
      startupAction =
        extension.startupActionClass.orNull?.let {
          IsolatedPolarisServerStartupAction.start(
            implementationClass = it,
            classpath = extension.startupActionClasspath.files,
            parameters = extension.startupActionParameters.get(),
            systemProperties = systemProperties,
            environment = environment,
          )
        }
      val started =
        RunningPolarisServer.start(
          javaExecutable = javaExecutable,
          jar = jar,
          workingDirectory = extension.workingDirectory.get().asFile,
          startupTimeout = extension.startupTimeout.get(),
          stopTimeout = extension.stopTimeout.get(),
          jvmArguments = extension.jvmArguments.get(),
          systemProperties = systemProperties,
          environment = environment,
          arguments = extension.arguments.get(),
          logger = logger,
        )
      extension.service.get().register(this, started.server)
      dynamicArguments.arguments = buildList {
        add("-D${extension.httpPortProperty.get()}=${started.ports.httpPort}")
        started.ports.managementPort?.let { add("-D${extension.managementPortProperty.get()}=$it") }
      }
    } catch (e: Throwable) {
      startupAction?.closeQuietly()
      startupAction = null
      throw e
    }
  }
}

private class PolarisServerArguments : CommandLineArgumentProvider {
  @get:Internal var arguments: List<String> = emptyList()

  override fun asArguments(): Iterable<String> = arguments
}

private class DefaultPolarisServerStartupContext(
  private val parameters: Map<String, String>,
  private val systemProperties: MutableMap<String, String>,
  private val environment: MutableMap<String, String>,
) : PolarisServerStartupContext {
  override fun getParameters(): Map<String, String> = parameters

  override fun getSystemProperties(): MutableMap<String, String> = systemProperties

  override fun getEnvironment(): MutableMap<String, String> = environment
}

private class IsolatedPolarisServerStartupAction(
  private val classLoader: URLClassLoader,
  private val delegate: PolarisServerStartupAction,
) : AutoCloseable {
  override fun close() {
    withContextClassLoader(classLoader) { delegate.close() }
    classLoader.close()
  }

  companion object {
    fun start(
      implementationClass: String,
      classpath: Set<File>,
      parameters: Map<String, String>,
      systemProperties: MutableMap<String, String>,
      environment: MutableMap<String, String>,
    ): IsolatedPolarisServerStartupAction {
      val urls = classpath.map { it.toURI().toURL() }.toTypedArray()
      val classLoader = URLClassLoader(urls, PolarisServerStartupAction::class.java.classLoader)
      val action =
        try {
          withContextClassLoader(classLoader) {
            val type = Class.forName(implementationClass, true, classLoader)
            type.getDeclaredConstructor().newInstance() as PolarisServerStartupAction
          }
        } catch (e: Throwable) {
          classLoader.close()
          throw GradleException(
            "Failed to load Polaris server startup action $implementationClass",
            e,
          )
        }
      val isolated = IsolatedPolarisServerStartupAction(classLoader, action)
      try {
        withContextClassLoader(classLoader) {
          action.start(
            DefaultPolarisServerStartupContext(parameters, systemProperties, environment)
          )
        }
      } catch (e: Throwable) {
        isolated.closeQuietly()
        throw GradleException("Failed to run Polaris server startup action $implementationClass", e)
      }
      return isolated
    }
  }
}

private fun AutoCloseable.closeQuietly() {
  try {
    close()
  } catch (_: Exception) {}
}

private fun <T> withContextClassLoader(classLoader: ClassLoader, action: () -> T): T {
  val thread = Thread.currentThread()
  val previous = thread.contextClassLoader
  thread.contextClassLoader = classLoader
  try {
    return action()
  } finally {
    thread.contextClassLoader = previous
  }
}
