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
package org.apache.polaris.service.it.ext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.polaris.service.it.env.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExternalPolarisServerManagerTest {

  private String originalHost;
  private String originalPort;

  @BeforeEach
  void captureProperties() {
    originalHost = System.getProperty(ExternalPolarisServerManager.HOST_PROPERTY);
    originalPort = System.getProperty(ExternalPolarisServerManager.HTTP_PORT_PROPERTY);
  }

  @AfterEach
  void restoreProperties() {
    restore(ExternalPolarisServerManager.HOST_PROPERTY, originalHost);
    restore(ExternalPolarisServerManager.HTTP_PORT_PROPERTY, originalPort);
  }

  @Test
  void serverForContextUsesConfiguredHttpPort() {
    System.setProperty(ExternalPolarisServerManager.HTTP_PORT_PROPERTY, "12345");

    Server server = new ExternalPolarisServerManager().serverForContext(null);

    assertThat(server.baseUri()).hasToString("http://localhost:12345");
    assertThat(server.adminCredentials().principalName()).isEqualTo("root");
    assertThat(server.adminCredentials().credentials().clientId()).isEqualTo("test-admin");
    assertThat(server.adminCredentials().credentials().clientSecret()).isEqualTo("test-secret");
  }

  @Test
  void serverForContextUsesConfiguredHost() {
    System.setProperty(ExternalPolarisServerManager.HTTP_PORT_PROPERTY, "12345");
    System.setProperty(ExternalPolarisServerManager.HOST_PROPERTY, "127.0.0.1");

    Server server = new ExternalPolarisServerManager().serverForContext(null);

    assertThat(server.baseUri()).hasToString("http://127.0.0.1:12345");
  }

  @Test
  void serverForContextRejectsMissingHttpPort() {
    System.clearProperty(ExternalPolarisServerManager.HTTP_PORT_PROPERTY);

    assertThatThrownBy(() -> new ExternalPolarisServerManager().serverForContext(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ExternalPolarisServerManager.HTTP_PORT_PROPERTY);
  }

  @Test
  void serverForContextRejectsInvalidHttpPort() {
    System.setProperty(ExternalPolarisServerManager.HTTP_PORT_PROPERTY, "not-a-port");

    assertThatThrownBy(() -> new ExternalPolarisServerManager().serverForContext(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ExternalPolarisServerManager.HTTP_PORT_PROPERTY);
  }

  @Test
  void serverForContextRejectsOutOfRangeHttpPort() {
    System.setProperty(ExternalPolarisServerManager.HTTP_PORT_PROPERTY, "65536");

    assertThatThrownBy(() -> new ExternalPolarisServerManager().serverForContext(null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(ExternalPolarisServerManager.HTTP_PORT_PROPERTY);
  }

  private static void restore(String propertyName, String value) {
    if (value == null) {
      System.clearProperty(propertyName);
    } else {
      System.setProperty(propertyName, value);
    }
  }
}
