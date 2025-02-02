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

import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.MapProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskCollection;
import org.gradle.api.tasks.TaskProvider;

public class PolarisRunnerExtension {
  private final MapProperty<String, String> environment;
  private final MapProperty<String, String> environmentNonInput;
  private final MapProperty<String, String> systemProperties;
  private final MapProperty<String, String> systemPropertiesNonInput;
  private final ListProperty<String> arguments;
  private final ListProperty<String> argumentsNonInput;
  private final ListProperty<String> jvmArguments;
  private final ListProperty<String> jvmArgumentsNonInput;
  private final Property<Integer> javaVersion;
  private final Property<String> httpListenPortProperty;
  private final Property<String> httpListenUrlProperty;
  private final Property<String> managementListenPortProperty;
  private final Property<String> managementListenUrlProperty;
  private final RegularFileProperty executableJar;
  private final RegularFileProperty workingDirectory;
  private final Property<Long> timeToListenUrlMillis;
  private final Property<Long> timeToStopMillis;

  private final Provider<PolarisRunnerService> polarisRunnerServiceProvider;

  public PolarisRunnerExtension(
      Project project, Provider<PolarisRunnerService> polarisRunnerServiceProvider) {
    this.polarisRunnerServiceProvider = polarisRunnerServiceProvider;

    environment = project.getObjects().mapProperty(String.class, String.class);
    environmentNonInput = project.getObjects().mapProperty(String.class, String.class);
    systemProperties = project.getObjects().mapProperty(String.class, String.class);
    systemPropertiesNonInput = project.getObjects().mapProperty(String.class, String.class);
    arguments = project.getObjects().listProperty(String.class);
    argumentsNonInput = project.getObjects().listProperty(String.class);
    jvmArguments = project.getObjects().listProperty(String.class);
    jvmArgumentsNonInput = project.getObjects().listProperty(String.class);
    javaVersion = project.getObjects().property(Integer.class).convention(21);
    httpListenUrlProperty =
        project.getObjects().property(String.class).convention("quarkus.http.test-url");
    httpListenPortProperty =
        project.getObjects().property(String.class).convention("quarkus.http.test-port");
    managementListenUrlProperty =
        project.getObjects().property(String.class).convention("quarkus.management.test-url");
    managementListenPortProperty =
        project.getObjects().property(String.class).convention("quarkus.management.test-port");
    workingDirectory =
        project
            .getObjects()
            .fileProperty()
            .convention(project.getLayout().getBuildDirectory().file("polaris-quarkus-server"));
    executableJar = project.getObjects().fileProperty();
    timeToListenUrlMillis = project.getObjects().property(Long.class).convention(0L);
    timeToStopMillis = project.getObjects().property(Long.class).convention(0L);
  }

  /** System properties for the Polaris JVM. */
  public MapProperty<String, String> getSystemProperties() {
    return systemProperties;
  }

  /** System properties for the Polaris JVM, not respected for Gradle build caching. */
  public MapProperty<String, String> getSystemPropertiesNonInput() {
    return systemPropertiesNonInput;
  }

  /** Environment variables for the Polaris JVM. */
  public MapProperty<String, String> getEnvironment() {
    return environment;
  }

  /** Environment variables for the Polaris JVM, not respected for Gradle build caching. */
  public MapProperty<String, String> getEnvironmentNonInput() {
    return environmentNonInput;
  }

  /** Arguments used to start the Polaris JVM. */
  public ListProperty<String> getArguments() {
    return arguments;
  }

  /** Arguments used to start the Polaris JVM, not respected for Gradle build caching. */
  public ListProperty<String> getArgumentsNonInput() {
    return argumentsNonInput;
  }

  /** JVM arguments used to start the Polaris JVM. */
  public ListProperty<String> getJvmArguments() {
    return jvmArguments;
  }

  /** JVM arguments used to start the Polaris JVM, not respected for Gradle build caching. */
  public ListProperty<String> getJvmArgumentsNonInput() {
    return jvmArgumentsNonInput;
  }

  /** The Java version to use to run Polaris, defaults to 21. */
  public Property<Integer> getJavaVersion() {
    return javaVersion;
  }

  /** The name of the property that will receive the HTTP port number. */
  public Property<String> getHttpListenPortProperty() {
    return httpListenPortProperty;
  }

  /**
   * The name of the property that will receive the HTTP listen URL, in the exact form as emitted by
   * Quarkus, likely containing {@code 0.0.0.0} has the host.
   */
  public Property<String> getHttpListenUrlProperty() {
    return httpListenUrlProperty;
  }

  /** The name of the property that will receive the management port number. */
  public Property<String> getManagementListenPortProperty() {
    return managementListenPortProperty;
  }

  /**
   * The name of the property that will receive the management listen URL, in the exact form as
   * emitted by Quarkus, likely containing {@code 0.0.0.0} has the host.
   */
  public Property<String> getManagementListenUrlProperty() {
    return managementListenUrlProperty;
  }

  /** The file of the executable jar to run Polaris. */
  public RegularFileProperty getExecutableJar() {
    return executableJar;
  }

  /**
   * Working directory used when starting Polaris, defaults to {@code build/polaris-quarkus-server}
   * in the current Gradle project.
   */
  public RegularFileProperty getWorkingDirectory() {
    return workingDirectory;
  }

  /**
   * Time to wait until the plugin expects Quarkus to emit the listen URLs, defaults to 30 seconds.
   */
  public Property<Long> getTimeToListenUrlMillis() {
    return timeToListenUrlMillis;
  }

  /**
   * Time to wait until Polaris has stopped after the termination signal, defaults to 15 seconds.
   */
  public Property<Long> getTimeToStopMillis() {
    return timeToStopMillis;
  }

  /**
   * The Gradle tasks of the current Gradle project to "decorate" with a running Polaris server,
   * with the HTTP and management URL and port properties.
   */
  public PolarisRunnerExtension includeTasks(TaskCollection<? extends Task> taskCollection) {
    return includeTasks(taskCollection, null);
  }

  /**
   * The Gradle tasks of the current Gradle project to "decorate" with a running Polaris server,
   * with the HTTP and management URL and port properties.
   */
  public <T extends Task> PolarisRunnerExtension includeTasks(
      TaskCollection<T> taskCollection, Action<T> postStartAction) {
    taskCollection.configureEach(
        new PolarisRunnerTaskConfigurer<>(postStartAction, polarisRunnerServiceProvider));
    return this;
  }

  /**
   * The Gradle tasks of the current Gradle project to "decorate" with a running Polaris server,
   * with the HTTP and management URL and port properties.
   */
  public PolarisRunnerExtension includeTask(TaskProvider<? extends Task> taskProvider) {
    return includeTask(taskProvider, null);
  }

  /**
   * The Gradle tasks of the current Gradle project to "decorate" with a running Polaris server,
   * with the HTTP and management URL and port properties.
   */
  public <T extends Task> PolarisRunnerExtension includeTask(
      TaskProvider<T> taskProvider, Action<T> postStartAction) {
    taskProvider.configure(
        new PolarisRunnerTaskConfigurer<>(postStartAction, polarisRunnerServiceProvider));
    return this;
  }
}
