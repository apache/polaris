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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.descriptor.PluginDescriptor;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.toolchain.ToolchainManager;
import org.apache.polaris.apprunner.common.JavaVM;
import org.apache.polaris.apprunner.common.ProcessHandler;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;

/** Starting Quarkus application. */
@Mojo(name = "start", requiresDependencyResolution = ResolutionScope.NONE, threadSafe = true)
public class PolarisRunnerStartMojo extends AbstractPolarisRunnerMojo {

  /** The entry point to Aether, i.e. the component doing all the work. */
  @Inject private RepositorySystem repoSystem;

  @Inject private ToolchainManager toolchainManager;

  /** The current repository/network configuration of Maven. */
  @Parameter(defaultValue = "${repositorySystemSession}", readonly = true)
  private RepositorySystemSession repoSession;

  /**
   * The project's remote repositories to use for the resolution of plugins and their dependencies.
   */
  @Parameter(defaultValue = "${project.remotePluginRepositories}", readonly = true)
  private List<RemoteRepository> remoteRepos;

  /** The plugin descriptor. */
  @Parameter(defaultValue = "${plugin}", readonly = true)
  @SuppressWarnings("unused")
  private PluginDescriptor pluginDescriptor;

  /**
   * The application artifact id.
   *
   * <p>Needs to be present as a plugin dependency, if "executableJar" is not set.
   *
   * <p>Mutually exclusive with "executableJar".
   *
   * <p>Supported format is groupId:artifactId[:type[:classifier]]:version
   */
  @Parameter(property = "polaris.apprunner.appArtifactId")
  private String appArtifactId;

  /** Environment variable configuration properties. */
  @Parameter private Properties systemProperties = new Properties();

  /**
   * Properties to get from Quarkus running application.
   *
   * <p>The property key is the name of the build property to set, the value is the name of the
   * quarkus configuration key to get.
   */
  @Parameter private Properties environment;

  @Parameter private List<String> arguments;

  @Parameter private List<String> jvmArguments;

  @Parameter(defaultValue = "21")
  private int javaVersion;

  /**
   * The path to the executable jar to run.
   *
   * <p>Mutually exclusive with "appArtifactId"
   */
  @Parameter private String executableJar;

  @Parameter(defaultValue = "quarkus.http.test-port")
  private String httpListenPortProperty;

  @Parameter(defaultValue = "quarkus.http.test-url")
  private String httpListenUrlProperty;

  @Parameter(defaultValue = "quarkus.management.test-port")
  private String managementListenPortProperty;

  @Parameter(defaultValue = "quarkus.management.test-url")
  private String managementListenUrlProperty;

  @Parameter(defaultValue = "${build.directory}/polaris-quarkus")
  private String workingDirectory;

  @Parameter private long timeToListenUrlMillis;

  @Parameter private long timeToStopMillis;

  static String noJavaVMMessage(int version) {
    return String.format(
        "Could not find a Java-VM for Java version %d. "
            + "Either configure a type=jdk in Maven's toolchain with version=%d or "
            + "set the Java-Home for a compatible JVM using the environment variable JDK%d_HOME or "
            + "JAVA%d_HOME.",
        version, version, version, version);
  }

  @Override
  public void execute() throws MojoExecutionException {
    if (isSkipped()) {
      getLog().debug("Execution is skipped");
      return;
    }

    getLog().debug(String.format("Searching for Java %d ...", javaVersion));
    String javaExecutable =
        toolchainManager
            .getToolchains(
                getSession(),
                "jdk",
                Collections.singletonMap("version", Integer.toString(javaVersion)))
            .stream()
            .map(tc -> tc.findTool("java"))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(
                () -> {
                  getLog()
                      .debug(
                          String.format(
                              "... using JavaVM as Maven toolkit returned no toolchain "
                                  + "for type==jdk and version==%d",
                              javaVersion));
                  JavaVM javaVM = JavaVM.findJavaVM(javaVersion);
                  return javaVM != null ? javaVM.getJavaExecutable().toString() : null;
                });
    if (javaExecutable == null) {
      throw new MojoExecutionException(noJavaVMMessage(javaVersion));
    }
    getLog().debug(String.format("Using javaExecutable %s", javaExecutable));

    Path workDir = Paths.get(workingDirectory);
    if (!Files.isDirectory(workDir)) {
      try {
        Files.createDirectories(workDir);
      } catch (IOException e) {
        throw new MojoExecutionException(
            String.format("Failed to create working directory %s", workingDirectory), e);
      }
    }

    String execJar = executableJar;
    if (execJar == null && appArtifactId == null) {
      throw new MojoExecutionException(
          "Either appArtifactId or executableJar config option must be specified, prefer appArtifactId");
    }
    if (execJar == null) {
      Artifact artifact = new DefaultArtifact(appArtifactId);
      ArtifactRequest artifactRequest = new ArtifactRequest(artifact, remoteRepos, null);
      try {
        ArtifactResult result = repoSystem.resolveArtifact(repoSession, artifactRequest);
        execJar = result.getArtifact().getFile().toString();
      } catch (ArtifactResolutionException e) {
        throw new MojoExecutionException(
            String.format("Failed to resolve artifact %s", appArtifactId), e);
      }
    } else if (appArtifactId != null) {
      throw new MojoExecutionException(
          "The options appArtifactId and executableJar are mutually exclusive");
    }

    List<String> command = new ArrayList<>();
    command.add(javaExecutable);
    if (jvmArguments != null) {
      command.addAll(jvmArguments);
    }
    if (systemProperties != null) {
      systemProperties.forEach(
          (k, v) -> command.add(String.format("-D%s=%s", k.toString(), v.toString())));
    }
    command.add("-Dquarkus.http.port=0");
    command.add("-Dquarkus.management.port=0");
    command.add("-Dquarkus.log.level=INFO");
    command.add("-Dquarkus.log.console.level=INFO");
    command.add("-jar");
    command.add(execJar);
    if (arguments != null) {
      command.addAll(arguments);
    }

    getLog()
        .info(
            String.format(
                "Starting process: %s, additional env: %s",
                String.join(" ", command),
                environment != null
                    ? environment.entrySet().stream()
                        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", "))
                    : "<none>"));

    ProcessBuilder processBuilder = new ProcessBuilder().command(command);
    if (environment != null) {
      environment.forEach((k, v) -> processBuilder.environment().put(k.toString(), v.toString()));
    }
    processBuilder.directory(workDir.toFile());

    try {
      ProcessHandler processHandler = new ProcessHandler();
      if (timeToListenUrlMillis > 0L) {
        processHandler.setTimeToListenUrlMillis(timeToListenUrlMillis);
      }
      if (timeToStopMillis > 0L) {
        processHandler.setTimeStopMillis(timeToStopMillis);
      }
      processHandler.setStdoutTarget(line -> getLog().info(String.format("[output] %s", line)));
      processHandler.start(processBuilder);

      setApplicationHandle(processHandler);

      List<String> listenUrls = processHandler.getListenUrls();

      Properties projectProperties = getProject().getProperties();
      projectProperties.setProperty(httpListenUrlProperty, listenUrls.get(0));
      projectProperties.setProperty(
          httpListenPortProperty, Integer.toString(URI.create(listenUrls.get(0)).getPort()));
      if (listenUrls.get(1) != null) {
        projectProperties.setProperty(managementListenUrlProperty, listenUrls.get(1));
        projectProperties.setProperty(
            managementListenPortProperty,
            Integer.toString(URI.create(listenUrls.get(1)).getPort()));
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MojoExecutionException(String.format("Process-start interrupted: %s", command), e);
    } catch (Exception e) {
      throw new MojoExecutionException(String.format("Failed to start the process %s", command), e);
    }
  }
}
