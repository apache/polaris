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

import static com.soebes.itf.extension.assertj.MavenITAssertions.assertThat;

import com.soebes.itf.jupiter.extension.MavenCLIOptions;
import com.soebes.itf.jupiter.extension.MavenGoal;
import com.soebes.itf.jupiter.extension.MavenJupiterExtension;
import com.soebes.itf.jupiter.extension.MavenOption;
import com.soebes.itf.jupiter.extension.MavenRepository;
import com.soebes.itf.jupiter.extension.MavenTest;
import com.soebes.itf.jupiter.maven.MavenExecutionResult;

@MavenJupiterExtension
@MavenRepository
class ITPolarisMavenPlugin {

  @MavenTest
  @MavenGoal("verify")
  @MavenOption(MavenCLIOptions.ERRORS)
  void executionJarAndApplicationIdMissing(MavenExecutionResult result) {
    assertThat(result)
        .isFailure()
        .out()
        .error()
        .anyMatch(
            s ->
                s.contains(
                    "Either appArtifactId or executableJar config option must be specified, prefer appArtifactId"));
  }

  @MavenTest
  @MavenGoal("verify")
  @MavenOption(MavenCLIOptions.ERRORS)
  void executionJarAndApplicationIdSpecified(MavenExecutionResult result) {
    assertThat(result)
        .isFailure()
        .out()
        .error()
        .anyMatch(
            s -> s.contains("The options appArtifactId and executableJar are mutually exclusive"));
  }

  @MavenTest
  @MavenGoal("verify")
  @MavenOption(MavenCLIOptions.ERRORS)
  void applicationIdSpecified(MavenExecutionResult result) {
    assertThat(result)
        .isSuccessful()
        .out()
        .info()
        .anyMatch(
            s ->
                s.matches(
                    "Starting process: .*-jar .*/nessie-quarkus-.*-runner.jar, additional env: .*HELLO=world.*"))
        .anyMatch(s -> s.matches("Starting process: .*java.* -Dfoo=bar .*"))
        .anyMatch(s -> s.matches("Starting process: .*java.* -Dquarkus.http.port=0 .*"))
        .anyMatch(s -> s.matches("Quarkus application stopped."));
  }

  @MavenTest
  @MavenGoal("verify")
  @MavenOption(MavenCLIOptions.ERRORS)
  @MavenOption(value = MavenCLIOptions.TOOLCHAINS, parameter = "non-java-toolchains.xml")
  void unknownJdk(MavenExecutionResult result) {
    assertThat(result)
        .isFailure()
        .out()
        .error()
        .anyMatch(s -> s.contains(PolarisRunnerStartMojo.noJavaVMMessage(42)));
  }
}
