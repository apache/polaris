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

import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.apache.polaris.apprunner.common.ProcessHandler;

/*
 * Base class to share configuration between mojo.
 */
abstract class AbstractPolarisRunnerMojo extends AbstractMojo {
  private static final String CONTEXT_KEY = "polaris.quarkus.app";

  /** Maven project. */
  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  /** Maven session. */
  @Parameter(defaultValue = "${session}", readonly = true, required = true)
  private MavenSession session;

  /** Whether execution should be skipped. */
  @Parameter(property = "polaris.apprunner.skip", required = false, defaultValue = "false")
  private boolean skip;

  /** Execution id for the app. */
  @Parameter(property = "polaris.apprunner.executionId", required = false, defaultValue = "default")
  private String executionId;

  public boolean isSkipped() {
    return skip;
  }

  public String getExecutionId() {
    return executionId;
  }

  public MavenProject getProject() {
    return project;
  }

  public MavenSession getSession() {
    return session;
  }

  private String getContextKey() {
    final String key = CONTEXT_KEY + '.' + getExecutionId();
    return key;
  }

  protected ProcessHandler getApplication() {
    final String key = getContextKey();
    return (ProcessHandler) project.getContextValue(key);
  }

  protected void resetApplication() {
    final String key = getContextKey();
    project.setContextValue(key, null);
  }

  protected void setApplicationHandle(ProcessHandler application) {
    final String key = getContextKey();
    final Object previous = project.getContextValue(key);
    if (previous != null) {
      getLog()
          .warn(
              String.format(
                  "Found a previous ProcessHandler for execution id %s.", getExecutionId()));
    }
    project.setContextValue(key, application);
  }
}
