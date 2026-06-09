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

import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.URI
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.gradle.api.GradleException
import org.gradle.api.logging.Logger

internal class RunningPolarisServer(
  private val process: Process,
  private val outputThread: Thread,
  private val stopTimeout: Duration,
) {
  fun stop(logger: Logger) {
    if (!process.isAlive) {
      joinOutputThread(logger)
      return
    }

    process.destroy()
    if (!process.waitFor(stopTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
      process.destroyForcibly()
      process.waitFor(stopTimeout.toMillis(), TimeUnit.MILLISECONDS)
    }
    joinOutputThread(logger)
  }

  private fun joinOutputThread(logger: Logger) {
    try {
      outputThread.join(stopTimeout.toMillis())
    } catch (e: InterruptedException) {
      Thread.currentThread().interrupt()
      logger.warn("Interrupted while waiting for Polaris server output thread to finish", e)
    }
  }

  companion object {
    private val listenPattern =
      Regex(
        "^.*Listening on: (https?://\\S+?)(?:[.] Management interface listening on (https?://\\S+?)[.])?\\s*$"
      )

    fun start(
      javaExecutable: String,
      jar: File,
      workingDirectory: File,
      startupTimeout: Duration,
      stopTimeout: Duration,
      jvmArguments: List<String>,
      systemProperties: Map<String, String>,
      environment: Map<String, String>,
      arguments: List<String>,
      logger: Logger,
    ): StartedServer {
      workingDirectory.mkdirs()
      val command = buildList {
        add(javaExecutable)
        addAll(jvmArguments)
        add("-Dquarkus.http.port=0")
        add("-Dquarkus.management.port=0")
        systemProperties.forEach { (name, value) -> add("-D$name=$value") }
        add("-jar")
        add(jar.absolutePath)
        addAll(arguments)
      }

      logger.info("Starting Polaris server process: {}", command)
      val processBuilder =
        ProcessBuilder(command).directory(workingDirectory).redirectErrorStream(true)
      processBuilder.environment().putAll(environment)
      val process = processBuilder.start()
      val detected = AtomicReference<ListenUrls>()
      val failed = AtomicReference<Throwable>()
      val ready = CountDownLatch(1)
      val capturedOutput = mutableListOf<String>()

      val outputThread =
        Thread(
          {
            try {
              BufferedReader(InputStreamReader(process.inputStream)).useLines { lines ->
                lines.forEach { line ->
                  logger.info("[polaris-server] {}", line)
                  synchronized(capturedOutput) { capturedOutput.add(line) }
                  if (detected.get() == null) {
                    val match = listenPattern.matchEntire(line)
                    if (match != null) {
                      detected.set(
                        ListenUrls(
                          match.groupValues[1],
                          match.groupValues.getOrNull(2).orEmpty().ifEmpty { null },
                        )
                      )
                      ready.countDown()
                    }
                  }
                }
              }
            } catch (e: Throwable) {
              failed.set(e)
              ready.countDown()
            } finally {
              ready.countDown()
            }
          },
          "polaris-server-output",
        )
      outputThread.isDaemon = true
      outputThread.start()

      if (!ready.await(startupTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        process.destroyForcibly()
        throw GradleException(
          "Polaris server did not emit a listen URL within $startupTimeout. Captured output:\n${capturedOutput(capturedOutput)}"
        )
      }

      failed.get()?.let { throw GradleException("Failed while reading Polaris server output", it) }
      val urls = detected.get()
      if (urls == null) {
        val exit =
          if (process.isAlive) "still running" else "exited with code ${process.exitValue()}"
        process.destroyForcibly()
        throw GradleException(
          "Polaris server $exit before emitting a listen URL. Captured output:\n${capturedOutput(capturedOutput)}"
        )
      }

      return StartedServer(RunningPolarisServer(process, outputThread, stopTimeout), urls.toPorts())
    }

    private fun capturedOutput(lines: MutableList<String>): String =
      synchronized(lines) { lines.takeLast(200).joinToString("\n") }.ifBlank { "<no output>" }
  }
}

internal data class StartedServer(val server: RunningPolarisServer, val ports: ServerPorts)

private data class ListenUrls(val httpUrl: String, val managementUrl: String?) {
  fun toPorts(): ServerPorts =
    ServerPorts(URI.create(httpUrl).port, managementUrl?.let { URI.create(it).port })
}

internal data class ServerPorts(val httpPort: Int, val managementPort: Int?)
