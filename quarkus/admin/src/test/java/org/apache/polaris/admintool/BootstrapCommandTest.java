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
package org.apache.polaris.admintool;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainTest;
import org.apache.polaris.core.persistence.PolarisCredentialsBootstrap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusMainTest
class BootstrapCommandTest {

  @Test
  @Launch(
      value = {
        "bootstrap",
        "-c",
        "[{\"realm\":\"realm1\",\"principal\":\"root\",\"clientId\":\"root\",\"clientSecret\":\"s3cr3t\"}]"
      })
  public void testBootstrap(LaunchResult result) {
    assertThat(result.getOutput()).contains("Bootstrap completed successfully.");
  }

  @Test
  @Launch(
      value = {
          "bootstrap",
          "-c",
          "[{\"realm\":\"realm1\",\"principal\":\"root\",\"clientId\":\"root\",\"clientSecret\":\"s3cr3t\"}]",
          "--print-credentials"
      })
  public void testPrintCredentials(LaunchResult result) {
    assertThat(result.getOutput()).contains("Bootstrap completed successfully.");
    assertThat(result.getOutput()).contains("root:");
  }

  @Test
  @Launch(
      value = {
          "bootstrap",
          "--print-credentials"
      })
  public void testPrintGeneratedCredentials(LaunchResult result) {
    assertThat(result.getOutput()).contains("Bootstrap completed successfully.");
    assertThat(result.getOutput()).doesNotContain("root:");
    assertThat(result.getOutput()).contains("root principal");
  }
}
