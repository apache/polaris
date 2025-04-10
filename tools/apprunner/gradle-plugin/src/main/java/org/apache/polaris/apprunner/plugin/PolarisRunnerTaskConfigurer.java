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

import static org.apache.polaris.apprunner.plugin.PolarisRunnerPlugin.APP_CONFIG_NAME;

import jakarta.annotation.Nonnull;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.gradle.api.Action;
import org.gradle.api.Task;
import org.gradle.api.file.RegularFile;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.PathSensitivity;

/** Configures the task for which the Polaris-Quarkus process shall be started. */
public class PolarisRunnerTaskConfigurer<T extends Task> implements Action<T> {

  private final Action<T> postStartAction;
  private final Provider<PolarisRunnerService> polarisRunnerServiceProvider;

  public PolarisRunnerTaskConfigurer(
      Action<T> postStartAction, Provider<PolarisRunnerService> polarisRunnerServiceProvider) {
    this.postStartAction = postStartAction;
    this.polarisRunnerServiceProvider = polarisRunnerServiceProvider;
  }

  @SuppressWarnings(
      "Convert2Lambda") // Gradle complains when using lambdas (build-cache won't wonk)
  @Override
  public void execute(T task) {
    var project = task.getProject();

    var appConfig = project.getConfigurations().getByName(APP_CONFIG_NAME);
    var extension = project.getExtensions().getByType(PolarisRunnerExtension.class);

    // Add the StartTask's properties as "inputs" to the Test task, so the Test task is
    // executed, when those properties change.
    var inputs = task.getInputs();
    inputs.properties(extension.getEnvironment().get());
    inputs.properties(extension.getSystemProperties().get());
    inputs.property("polaris.quarkus.arguments", extension.getArguments().get().toString());
    inputs.property("polaris.quarkus.jvmArguments", extension.getJvmArguments().get().toString());
    RegularFile execJar = extension.getExecutableJar().getOrNull();
    if (execJar != null) {
      inputs.file(execJar).withPathSensitivity(PathSensitivity.RELATIVE);
    }
    inputs.property("polaris.quarkus.javaVersion", extension.getJavaVersion().get());

    inputs.files(appConfig);

    var dependencies = appConfig.getDependencies();
    // Although we assert that only a single artifact is used (later), collect all dependencies
    // for a nicer error message.
    var dependenciesString =
        dependencies.stream()
            .map(d -> String.format("%s:%s:%s", d.getGroup(), d.getName(), d.getVersion()))
            .collect(Collectors.joining(", "));
    var files =
        !dependencies.isEmpty()
            ? appConfig.getIncoming().artifactView(viewConfiguration -> {}).getFiles()
            : null;

    var extra = task.getExtensions().findByType(ExtraPropertiesExtension.class);
    BiConsumer<String, String> httpUrlAndPortConsumer =
        extra != null
            ? (listenUrl, listenPort) -> {
              extra.set(extension.getHttpListenUrlProperty().get(), listenUrl);
              extra.set(extension.getHttpListenPortProperty().get(), listenPort);
            }
            : (listenUrl, listenPort) -> {};
    BiConsumer<String, String> managementUrlAndPortConsumer =
        extra != null
            ? (listenUrl, listenPort) -> {
              extra.set(extension.getManagementListenUrlProperty().get(), listenUrl);
              extra.set(extension.getManagementListenPortProperty().get(), listenPort);
            }
            : (listenUrl, listenPort) -> {};

    if (extra != null) {
      task.notCompatibleWithConfigurationCache(
          "PolarisRunner needs Gradle's extra-properties, which is incompatible with the configuration cache");
    }

    // Start the Polaris-Quarkus-App only when the Test task actually runs

    task.usesService(polarisRunnerServiceProvider);
    task.doFirst(
        new Action<>() {
          @SuppressWarnings("unchecked")
          @Override
          public void execute(@Nonnull Task t) {
            var processState = new ProcessState();
            polarisRunnerServiceProvider.get().register(processState, t);

            processState.quarkusStart(
                t,
                extension,
                files,
                dependenciesString,
                httpUrlAndPortConsumer,
                managementUrlAndPortConsumer);
            if (postStartAction != null) {
              postStartAction.execute((T) t);
            }
          }
        });
    task.doLast(
        new Action<>() {
          @Override
          public void execute(@Nonnull Task t) {
            polarisRunnerServiceProvider.get().finished(t);
          }
        });
  }
}
