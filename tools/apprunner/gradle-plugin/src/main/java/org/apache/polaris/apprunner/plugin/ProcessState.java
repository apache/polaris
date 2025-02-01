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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.polaris.apprunner.common.JavaVM;
import org.apache.polaris.apprunner.common.ProcessHandler;
import org.gradle.api.GradleException;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.JavaForkOptions;
import org.slf4j.Logger;

public class ProcessState {

  private ProcessHandler processHandler;

  public ProcessState() {
    // intentionally empty
  }

  @TaskAction
  public void noop() {}

  void quarkusStart(
      Task task,
      PolarisRunnerExtension extension,
      FileCollection appConfigFiles,
      String dependenciesString,
      BiConsumer<String, String> httpUrlAndPortConsumer,
      BiConsumer<String, String> managementUrlAndPortConsumer) {

    RegularFile configuredJar = extension.getExecutableJar().getOrNull();

    File execJar;

    if (configuredJar == null) {
      if (appConfigFiles != null && !appConfigFiles.isEmpty()) {
        var appConfigFileSet = appConfigFiles.getFiles();
        if (appConfigFileSet.size() != 1) {
          throw new GradleException(
              String.format(
                  "Expected configuration %s to resolve to exactly one artifact, but resolves to %s (hint: do not enable transitive on the dependency)",
                  PolarisRunnerPlugin.APP_CONFIG_NAME, dependenciesString));
        }
        execJar = appConfigFileSet.iterator().next();
      } else {
        throw new GradleException(
            String.format(
                "Neither does the configuration %s contain exactly one dependency (preferably org.apache.polaris:polaris-quarkus-server:runner), nor is the runner jar specified in the %s extension.",
                PolarisRunnerPlugin.APP_CONFIG_NAME, PolarisRunnerPlugin.EXTENSION_NAME));
      }
    } else {
      if (appConfigFiles != null && !appConfigFiles.isEmpty()) {
        throw new GradleException(
            String.format(
                "Configuration %s contains a dependency and option 'executableJar' are mutually exclusive",
                PolarisRunnerPlugin.APP_CONFIG_NAME));
      }
      execJar = configuredJar.getAsFile();
    }

    var javaVM = JavaVM.findJavaVM(extension.getJavaVersion().get());
    if (javaVM == null) {
      throw new GradleException(noJavaMessage(extension.getJavaVersion().get()));
    }

    var workDir = extension.getWorkingDirectory().getAsFile().get().toPath();
    if (!Files.isDirectory(workDir)) {
      try {
        Files.createDirectories(workDir);
      } catch (IOException e) {
        throw new GradleException(
            String.format("Failed to create working directory %s", workDir), e);
      }
    }

    var command = new ArrayList<String>();
    command.add(javaVM.getJavaExecutable().toString());
    command.addAll(extension.getJvmArguments().get());
    command.addAll(extension.getJvmArgumentsNonInput().get());
    command.add("-Dquarkus.http.port=0");
    command.add("-Dquarkus.management.port=0");
    command.add("-Dquarkus.log.level=INFO");
    command.add("-Dquarkus.log.console.level=INFO");
    extension
        .getSystemProperties()
        .get()
        .forEach((k, v) -> command.add(String.format("-D%s=%s", k, v)));
    extension
        .getSystemPropertiesNonInput()
        .get()
        .forEach((k, v) -> command.add(String.format("-D%s=%s", k, v)));
    command.add("-jar");
    command.add(execJar.getAbsolutePath());
    command.addAll(extension.getArguments().get());
    command.addAll(extension.getArgumentsNonInput().get());

    task.getLogger().info("Starting process: {}", command);

    var processBuilder = new ProcessBuilder().command(command);
    extension.getEnvironment().get().forEach((k, v) -> processBuilder.environment().put(k, v));
    extension
        .getEnvironmentNonInput()
        .get()
        .forEach((k, v) -> processBuilder.environment().put(k, v));
    processBuilder.directory(workDir.toFile());

    var logger = task.getLogger();

    try {
      processHandler = new ProcessHandler();
      processHandler.setStdoutTarget(line -> logger.info("[output] {}", line));
      processHandler.start(processBuilder);
      if (extension.getTimeToListenUrlMillis().get() > 0L) {
        processHandler.setTimeToListenUrlMillis(extension.getTimeToListenUrlMillis().get());
      }
      if (extension.getTimeToStopMillis().get() > 0L) {
        processHandler.setTimeStopMillis(extension.getTimeToStopMillis().get());
      }
      processHandler.getListenUrls();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new GradleException(String.format("Process-start interrupted: %s", command), e);
    } catch (TimeoutException e) {
      throw new GradleException(
          String.format("Polaris-Server/Quarkus did not emit listen URL. Process: %s", command), e);
    } catch (IOException e) {
      throw new GradleException(String.format("Failed to start the process %s", command), e);
    }

    List<String> listenUrls;
    try {
      listenUrls = processHandler.getListenUrls();
    } catch (Exception e) {
      // Can safely ignore it (it invocation does not block and therefore not throw an exception).
      // But make the IDE happy with this throw.
      throw new RuntimeException(e);
    }

    var httpListenUrl = listenUrls.getFirst();
    String httpListenPort = Integer.toString(URI.create(httpListenUrl).getPort());
    // Add the Quarkus properties as "generic properties", so any task can use them.
    httpUrlAndPortConsumer.accept(httpListenUrl, httpListenPort);

    List<String> jvmOpts;

    var managementListenUrl = listenUrls.get(1);
    if (managementListenUrl != null) {
      var managementListenPort = Integer.toString(URI.create(managementListenUrl).getPort());
      managementUrlAndPortConsumer.accept(managementListenUrl, managementListenPort);

      jvmOpts =
          Arrays.asList(
              String.format("-D%s=%s", extension.getHttpListenUrlProperty().get(), httpListenUrl),
              String.format("-D%s=%s", extension.getHttpListenPortProperty().get(), httpListenPort),
              String.format(
                  "-D%s=%s", extension.getManagementListenUrlProperty().get(), managementListenUrl),
              String.format(
                  "-D%s=%s",
                  extension.getManagementListenPortProperty().get(), managementListenPort));
    } else {
      jvmOpts =
          Arrays.asList(
              String.format("-D%s=%s", extension.getHttpListenUrlProperty().get(), httpListenUrl),
              String.format(
                  "-D%s=%s", extension.getHttpListenPortProperty().get(), httpListenPort));
    }

    // Do not put the "dynamic" properties (quarkus.http.test-port) to the `Test` task's
    // system-properties, because those are subject to the test-task's inputs, which is used
    // as the build-cache key. Instead, pass the dynamic properties via a
    // CommandLineArgumentProvider.
    // In other words: ensure that the `Test` tasks is cacheable.
    if (task instanceof JavaForkOptions test) {
      test.getJvmArgumentProviders().add(() -> jvmOpts);
    }
  }

  void quarkusStop(Logger logger) {
    if (processHandler == null) {
      logger.debug("No application found.");
      return;
    }

    try {
      processHandler.stop();
      logger.info("Quarkus application stopped.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      processHandler = null;
    }
  }

  static String noJavaMessage(int version) {
    return String.format(
        "Could not find a Java-VM for Java version %d. "
            + "Set the Java-Home for a compatible JVM using the environment variable JDK%d_HOME or "
            + "JAVA%d_HOME.",
        version, version, version);
  }
}
