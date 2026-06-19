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

import org.gradle.api.Task
import org.gradle.api.logging.Logging
import org.gradle.api.services.BuildService
import org.gradle.api.services.BuildServiceParameters

abstract class PolarisServerTestService : BuildService<BuildServiceParameters.None>, AutoCloseable {
  private val servers = linkedMapOf<String, RunningPolarisServer>()
  private val logger = Logging.getLogger(PolarisServerTestService::class.java)

  internal fun register(task: Task, server: RunningPolarisServer) {
    synchronized(servers) {
      if (servers.containsKey(task.path)) {
        throw IllegalStateException("Server already registered for task ${task.path}")
      }
      servers[task.path] = server
    }
  }

  fun finished(task: Task) {
    val server = synchronized(servers) { servers.remove(task.path) }
    server?.stop(task.logger)
  }

  override fun close() {
    val remaining = synchronized(servers) { servers.values.toList().also { servers.clear() } }
    if (remaining.isNotEmpty()) {
      logger.warn(
        "Stopping {} Polaris server process(es) during build service cleanup",
        remaining.size,
      )
    }
    remaining.forEach { it.stop(logger) }
  }
}
