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
package org.apache.polaris.service.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import org.apache.polaris.version.PolarisVersion;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

/**
 * Test to verify that the Quarkus application version is correctly configured to match the Polaris
 * project version. This ensures the version displayed in Quarkus startup logs reflects the actual
 * project version from version.txt rather than the Quarkus default of "1.0.0".
 *
 * @see <a href="https://github.com/apache/polaris/issues/3555">Issue #3555</a>
 */
@QuarkusTest
public class QuarkusApplicationVersionTest {

  @ConfigProperty(name = "quarkus.application.version")
  String quarkusApplicationVersion;

  @ConfigProperty(name = "quarkus.application.name")
  String quarkusApplicationName;

  @Test
  public void testQuarkusApplicationVersionMatchesPolarisVersion() {
    // The quarkus.application.version should match the Polaris version from version.txt
    String polarisVersion = PolarisVersion.polarisVersionString();
    assertThat(quarkusApplicationVersion)
        .as("Quarkus application version should match Polaris version from version.txt")
        .isEqualTo(polarisVersion);
  }

  @Test
  public void testQuarkusApplicationVersionIsNotDefault() {
    // The version should NOT be the Quarkus default "1.0.0"
    assertThat(quarkusApplicationVersion)
        .as("Quarkus application version should not be the default '1.0.0'")
        .isNotEqualTo("1.0.0");
  }

  @Test
  public void testQuarkusApplicationNameIsConfigured() {
    // Verify the application name is set correctly
    assertThat(quarkusApplicationName)
        .as("Quarkus application name should be configured")
        .isEqualTo("Apache Polaris Server (incubating)");
  }
}

