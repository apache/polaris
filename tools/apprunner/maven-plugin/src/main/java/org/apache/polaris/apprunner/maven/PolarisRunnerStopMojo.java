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
package org.apache.polaris.apprunner.maven;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.polaris.apprunner.common.ProcessHandler;

/** Stop Quarkus application. */
@Mojo(name = "stop", requiresDependencyResolution = ResolutionScope.NONE, threadSafe = true)
public class PolarisRunnerStopMojo extends AbstractPolarisRunnerMojo {
  /** Mojo execution. */
  @Override
  public void execute() throws MojoExecutionException {
    if (isSkipped()) {
      getLog().info("Stopping Quarkus application.");
      return;
    }

    ProcessHandler application = getApplication();
    if (application == null) {
      getLog().warn(String.format("No application found for execution id '%s'.", getExecutionId()));
      return;
    }

    try {
      application.stop();
      getLog().info("Quarkus application stopped.");
    } catch (Exception e) {
      throw new MojoExecutionException("Error while stopping Quarkus application", e);
    } finally {
      resetApplication();
    }
  }
}
