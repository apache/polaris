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
package org.apache.polaris.server.test.runner.spi;

/**
 * Isolated startup action that can prepare external services before the Polaris server process
 * starts.
 *
 * <p>Implementations are loaded from a child classloader configured by the build and may mutate the
 * provided context to pass task-specific system properties or environment variables to the Polaris
 * server. Implementations are closed after the decorated test task finishes, or when startup fails.
 */
public interface PolarisServerStartupAction extends AutoCloseable {
  /** Starts required services and updates the Polaris server startup context. */
  void start(PolarisServerStartupContext context) throws Exception;

  /** Stops resources owned by this action. */
  @Override
  default void close() throws Exception {}
}
