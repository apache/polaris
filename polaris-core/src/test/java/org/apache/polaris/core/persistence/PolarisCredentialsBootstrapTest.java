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

import java.util.Comparator;
import java.util.List;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.junit.jupiter.api.Test;

class PolarisCredentialsBootstrapTest {

  private final Comparator<PolarisPrincipalSecrets> comparator =
      (a, b) ->
          a.getPrincipalId() == b.getPrincipalId()
                  && a.getPrincipalClientId().equals(b.getPrincipalClientId())
                  && a.getMainSecret().equals(b.getMainSecret())
                  && a.getSecondarySecret().equals(b.getSecondarySecret())
              ? 0
              : 1;

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
  void duplicatePrincipal() {
    assertThatThrownBy(
            () ->
                PolarisCredentialsBootstrap.fromString(
                    "realm1,user1a,client1a,secret1a;realm1,user1a,client1b,secret1b"))
        .hasMessage("Duplicate principal: user1a");
  }

  @Test
  void getSecretsValidString() {
    PolarisCredentialsBootstrap credentials =
        PolarisCredentialsBootstrap.fromString(
            " ; realm1 , user1a , client1a , secret1a ; realm1 , user1b , client1b , secret1b ; realm2 , user2a , client2a , secret2a ; ");
    assertThat(credentials.getSecrets("realm1", 123, "nonexistent")).isEmpty();
    assertThat(credentials.getSecrets("nonexistent", 123, "user1a")).isEmpty();
    assertThat(credentials.getSecrets("realm1", 123, "user1a"))
        .usingValueComparator(comparator)
        .contains(new PolarisPrincipalSecrets(123, "client1a", "secret1a", "secret1a"));
    assertThat(credentials.getSecrets("realm1", 123, "user1b"))
        .usingValueComparator(comparator)
        .contains(new PolarisPrincipalSecrets(123, "client1b", "secret1b", "secret1b"));
    assertThat(credentials.getSecrets("realm2", 123, "user2a"))
        .usingValueComparator(comparator)
        .contains(new PolarisPrincipalSecrets(123, "client2a", "secret2a", "secret2a"));
  }

  @Test
  void getSecretsValidList() {
    PolarisCredentialsBootstrap credentials =
        PolarisCredentialsBootstrap.fromList(
            List.of(
                "realm1,user1a,client1a,secret1a",
                "realm1,user1b,client1b,secret1b",
                "realm2,user2a,client2a,secret2a"));
    assertThat(credentials.getSecrets("realm1", 123, "nonexistent")).isEmpty();
    assertThat(credentials.getSecrets("nonexistent", 123, "user1a")).isEmpty();
    assertThat(credentials.getSecrets("realm1", 123, "user1a"))
        .usingValueComparator(comparator)
        .contains(new PolarisPrincipalSecrets(123, "client1a", "secret1a", "secret1a"));
    assertThat(credentials.getSecrets("realm1", 123, "user1b"))
        .usingValueComparator(comparator)
        .contains(new PolarisPrincipalSecrets(123, "client1b", "secret1b", "secret1b"));
    assertThat(credentials.getSecrets("realm2", 123, "user2a"))
        .usingValueComparator(comparator)
        .contains(new PolarisPrincipalSecrets(123, "client2a", "secret2a", "secret2a"));
  }

  @Test
  void getSecretsValidSystemProperty() {
    PolarisCredentialsBootstrap credentials = PolarisCredentialsBootstrap.fromEnvironment();
    assertThat(credentials.credentials).isEmpty();
    try {
      System.setProperty("polaris.bootstrap.credentials", "realm1,user1a,client1a,secret1a");
      credentials = PolarisCredentialsBootstrap.fromEnvironment();
      assertThat(credentials.getSecrets("realm1", 123, "nonexistent")).isEmpty();
      assertThat(credentials.getSecrets("nonexistent", 123, "user1a")).isEmpty();
      assertThat(credentials.getSecrets("realm1", 123, "user1a"))
          .usingValueComparator(comparator)
          .contains(new PolarisPrincipalSecrets(123, "client1a", "secret1a", "secret1a"));
    } finally {
      System.clearProperty("polaris.bootstrap.credentials");
    }
  }
}
