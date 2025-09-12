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
package org.apache.polaris.core.persistence.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import org.junit.jupiter.api.Test;

class RootCredentialsSetTest {

  @Test
  void nullString() {
    RootCredentialsSet credentials = RootCredentialsSet.fromString(null);
    assertThat(credentials.credentials()).isEmpty();
  }

  @Test
  void emptyString() {
    RootCredentialsSet credentials = RootCredentialsSet.fromString("");
    assertThat(credentials.credentials()).isEmpty();
  }

  @Test
  void blankString() {
    RootCredentialsSet credentials = RootCredentialsSet.fromString("  ");
    assertThat(credentials.credentials()).isEmpty();
  }

  @Test
  void invalidString() {
    assertThatThrownBy(() -> RootCredentialsSet.fromString("test"))
        .hasMessage("Invalid credentials format: test");
  }

  @Test
  void duplicateRealm() {
    assertThatThrownBy(
            () ->
                RootCredentialsSet.fromString("realm1,client1a,secret1a;realm1,client1b,secret1b"))
        .hasMessage("Duplicate realm: realm1");
  }

  @Test
  void getSecretsValidString() {
    RootCredentialsSet credentials =
        RootCredentialsSet.fromString(
            " ; realm1 , client1 , secret1 ; realm2 , client2 , secret2 ; ");
    assertCredentials(credentials);
  }

  @Test
  void getSecretsValidList() {
    RootCredentialsSet credentials =
        RootCredentialsSet.fromList(List.of("realm1,client1,secret1", "realm2,client2,secret2"));
    assertCredentials(credentials);
  }

  @Test
  void getSecretsValidSystemProperty() {
    RootCredentialsSet credentials = RootCredentialsSet.fromEnvironment();
    assertThat(credentials.credentials()).isEmpty();
    try {
      System.setProperty(
          RootCredentialsSet.SYSTEM_PROPERTY, "realm1,client1,secret1;realm2,client2,secret2");
      credentials = RootCredentialsSet.fromEnvironment();
      assertCredentials(credentials);
    } finally {
      System.clearProperty(RootCredentialsSet.SYSTEM_PROPERTY);
    }
  }

  @Test
  void getSecretsValidJson() throws URISyntaxException {
    URL resource = getClass().getResource("credentials.json");
    assertThat(resource).isNotNull();
    RootCredentialsSet set = RootCredentialsSet.fromUri(resource.toURI());
    assertCredentials(set);
  }

  @Test
  void getSecretsValidYaml() throws URISyntaxException {
    URL resource = getClass().getResource("credentials.yaml");
    assertThat(resource).isNotNull();

    RootCredentialsSet set = RootCredentialsSet.fromUri(resource.toURI());
    assertCredentials(set);
  }

  @Test
  void getSecretsInvalidJson() {
    URL resource = getClass().getResource("credentials-invalid.json");
    assertThat(resource).isNotNull();
    assertThatThrownBy(() -> RootCredentialsSet.fromUri(resource.toURI()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to read credentials")
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Cannot build RootCredentials, some of required attributes are not set [clientId, clientSecret]");
  }

  @Test
  void getSecretsInvalidYaml() {
    URL resource = getClass().getResource("credentials-invalid.yaml");
    assertThat(resource).isNotNull();
    assertThatThrownBy(() -> RootCredentialsSet.fromUri(resource.toURI()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to read credentials")
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Cannot build RootCredentials, some of required attributes are not set [clientId, clientSecret]");
  }

  private static void assertCredentials(RootCredentialsSet set) {
    assertThat(set.credentials()).hasSize(2);
    assertThat(set.credentials().keySet()).containsExactlyInAnyOrder("realm1", "realm2");
    assertThat(set.credentials().get("realm1"))
        .isEqualTo(ImmutableRootCredentials.of("client1", "secret1"));
    assertThat(set.credentials().get("realm2"))
        .isEqualTo(ImmutableRootCredentials.of("client2", "secret2"));
  }
}
