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
package org.apache.polaris.core.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class PolarisCredentialsBootstrapTest {

  @Test
  void nullString() {
    PolarisCredentialsBootstrap credentials = PolarisCredentialsBootstrap.fromString(null);
    assertThat(credentials.credentials).isEmpty();
  }

  @Test
  void emptyString() {
    PolarisCredentialsBootstrap credentials = PolarisCredentialsBootstrap.fromString("");
    assertThat(credentials.credentials).isEmpty();
  }

  @Test
  void blankString() {
    PolarisCredentialsBootstrap credentials = PolarisCredentialsBootstrap.fromString("  ");
    assertThat(credentials.credentials).isEmpty();
  }

  @Test
  void invalidString() {
    assertThatThrownBy(() -> PolarisCredentialsBootstrap.fromString("test"))
        .hasMessage("Invalid credentials format: test");
  }

  @Test
  void duplicateRealm() {
    assertThatThrownBy(
            () ->
                PolarisCredentialsBootstrap.fromString(
                    "realm1,client1a,secret1a;realm1,client1b,secret1b"))
        .hasMessage("Duplicate realm: realm1");
  }

  @Test
  void getSecretsValidString() {
    PolarisCredentialsBootstrap credentials =
        PolarisCredentialsBootstrap.fromString(
            " ; realm1 , client1 , secret1 ; realm2 , client2 , secret2 ; ");
    assertCredentials(credentials);
  }

  @Test
  void getSecretsValidList() {
    PolarisCredentialsBootstrap credentials =
        PolarisCredentialsBootstrap.fromList(
            List.of("realm1,client1,secret1", "realm2,client2,secret2"));
    assertCredentials(credentials);
  }

  @Test
  void getSecretsValidSystemProperty() {
    PolarisCredentialsBootstrap credentials = PolarisCredentialsBootstrap.fromEnvironment();
    assertThat(credentials.credentials).isEmpty();
    try {
      System.setProperty(
          "polaris.bootstrap.credentials", "realm1,client1,secret1;realm2,client2,secret2");
      credentials = PolarisCredentialsBootstrap.fromEnvironment();
      assertCredentials(credentials);
    } finally {
      System.clearProperty("polaris.bootstrap.credentials");
    }
  }

  private void assertCredentials(PolarisCredentialsBootstrap credentials) {
    assertThat(credentials.getSecrets("realm3", 123, "root")).isEmpty();
    assertThat(credentials.getSecrets("nonexistent", 123, "root")).isEmpty();
    assertThat(credentials.getSecrets("realm1", 123, "non-root")).isEmpty();
    assertThat(credentials.getSecrets("realm1", 123, "root"))
        .hasValueSatisfying(
            secrets -> {
              assertThat(secrets.getPrincipalId()).isEqualTo(123);
              assertThat(secrets.getPrincipalClientId()).isEqualTo("client1");
              assertThat(secrets.getMainSecret()).isEqualTo("secret1");
              assertThat(secrets.getSecondarySecret()).isEqualTo("secret1");
            });
    assertThat(credentials.getSecrets("realm2", 123, "root"))
        .hasValueSatisfying(
            secrets -> {
              assertThat(secrets.getPrincipalId()).isEqualTo(123);
              assertThat(secrets.getPrincipalClientId()).isEqualTo("client2");
              assertThat(secrets.getMainSecret()).isEqualTo("secret2");
              assertThat(secrets.getSecondarySecret()).isEqualTo("secret2");
            });
  }
}
