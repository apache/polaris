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

import java.util.Map;

/** Context passed to an isolated {@link PolarisServerStartupAction}. */
public interface PolarisServerStartupContext {
  /**
   * Build-configured startup-action parameters.
   *
   * <p>These values come from the Gradle plugin's {@code startupActionParameters} map and are only
   * intended for communication between the build script and the startup action. They are not JVM
   * arguments, Quarkus configuration properties, or environment variables unless the startup action
   * explicitly copies them into {@link #getSystemProperties()} or {@link #getEnvironment()}.
   */
  Map<String, String> getParameters();

  /** Mutable system properties that will be passed to the Polaris server JVM. */
  Map<String, String> getSystemProperties();

  /** Mutable environment variables that will be passed to the Polaris server process. */
  Map<String, String> getEnvironment();
}
