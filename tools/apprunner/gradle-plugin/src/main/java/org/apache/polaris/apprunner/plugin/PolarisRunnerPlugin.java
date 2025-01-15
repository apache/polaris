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
package org.apache.polaris.apprunner.plugin;

import java.util.concurrent.ThreadLocalRandom;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class PolarisRunnerPlugin implements Plugin<Project> {

  static final String EXTENSION_NAME = "polarisQuarkusApp";

  /**
   * The name of the Gradle configuration that contains the Quarkus server application as the only
   * dependency.
   */
  static final String APP_CONFIG_NAME = "polarisQuarkusServer";

  @Override
  public void apply(Project project) {
    project
        .getConfigurations()
        .register(
            APP_CONFIG_NAME,
            c ->
                c.setTransitive(false)
                    .setDescription(
                        "References the Polaris-Quarkus server dependency, only a single dependency allowed."));

    var runnerService =
        project
            .getGradle()
            .getSharedServices()
            .registerIfAbsent(
                // Make the build-service unique per project to prevent Gradle class-cast
                // exceptions when the plugin's reloaded within the same build using different
                // class loaders.
                "polaris-quarkus-runner-" + ThreadLocalRandom.current().nextLong(),
                PolarisRunnerService.class,
                spec -> {});

    project
        .getExtensions()
        .create(EXTENSION_NAME, PolarisRunnerExtension.class, project, runnerService);
  }
}
