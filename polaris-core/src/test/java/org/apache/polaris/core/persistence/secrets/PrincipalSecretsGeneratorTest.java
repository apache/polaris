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
package org.apache.polaris.core.persistence.secrets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

import jakarta.annotation.Nullable;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PrincipalSecretsGeneratorTest {

  @Test
  void testRandomSecrets() {
    RandomPrincipalSecretsGenerator rpsg = new RandomPrincipalSecretsGenerator("realm");
    PolarisPrincipalSecrets s = rpsg.produceSecrets("name1", 123);
    assertThat(s).isNotNull();
    assertThat(s.getPrincipalId()).isEqualTo(123);
    assertThat(s.getPrincipalClientId()).isNotNull();
    assertThat(s.getMainSecret()).isNotNull();
    assertThat(s.getSecondarySecret()).isNotNull();
  }

  @Test
  void testRandomSecretsNullRealm() {
    RandomPrincipalSecretsGenerator rpsg = new RandomPrincipalSecretsGenerator(null);
    PolarisPrincipalSecrets s = rpsg.produceSecrets("name1", 123);
    assertThat(s).isNotNull();
    assertThat(s.getPrincipalId()).isEqualTo(123);
    assertThat(s.getPrincipalClientId()).isNotNull();
    assertThat(s.getMainSecret()).isNotNull();
    assertThat(s.getSecondarySecret()).isNotNull();
  }

  @Test
  void testEnvVariableSecrets() {
    EnvVariablePrincipalSecretsGenerator psg =
        Mockito.spy(new EnvVariablePrincipalSecretsGenerator("REALM"));

    String clientIdKey = "POLARIS_BOOTSTRAP_REALM_PRINCIPAL_CLIENT_ID";
    String clientSecretKey = "POLARIS_BOOTSTRAP_REALM_PRINCIPAL_CLIENT_SECRET";

    doReturn("test-id").when(psg).getEnvironmentVariable(clientIdKey);
    doReturn("test-secret").when(psg).getEnvironmentVariable(clientSecretKey);

    // Invoke the method
    PolarisPrincipalSecrets secrets = psg.produceSecrets("PRINCIPAL", 123);

    // Verify the result
    Assertions.assertNotNull(secrets);
    Assertions.assertEquals(123, secrets.getPrincipalId());
    Assertions.assertEquals("test-id", secrets.getPrincipalClientId());
    Assertions.assertEquals("test-secret", secrets.getMainSecret());
  }

  @Test
  void testBoostrapGeneratorDelegationToRandomPrincipalSecrets() {
    EnvVariablePrincipalSecretsGenerator mockedEnvVariablePrincipalSecretsGenerator =
        Mockito.spy(new EnvVariablePrincipalSecretsGenerator("REALM"));

    String clientIdKey = "POLARIS_BOOTSTRAP_REALM_PRINCIPAL_CLIENT_ID";
    String clientSecretKey = "POLARIS_BOOTSTRAP_REALM_PRINCIPAL_CLIENT_SECRET";

    doReturn("test-id")
        .when(mockedEnvVariablePrincipalSecretsGenerator)
        .getEnvironmentVariable(clientIdKey);
    doReturn("test-secret")
        .when(mockedEnvVariablePrincipalSecretsGenerator)
        .getEnvironmentVariable(clientSecretKey);

    class ExposingPrincipalSecretsGenerator extends BootstrapPrincipalSecretsGenerator {
      public ExposingPrincipalSecretsGenerator(@Nullable String realmName) {
        super(realmName);
      }

      @Override
      protected PrincipalSecretsGenerator buildEnvVariablePrincipalSecretsGenerator(
          String realmName) {
        return mockedEnvVariablePrincipalSecretsGenerator;
      }

      public PrincipalSecretsGenerator seeDelegate(String principalName) {
        return this.getDelegate(principalName);
      }
    }

    ExposingPrincipalSecretsGenerator fallback = new ExposingPrincipalSecretsGenerator(null);
    Assertions.assertInstanceOf(RandomPrincipalSecretsGenerator.class, fallback.seeDelegate("p"));

    ExposingPrincipalSecretsGenerator hasVars = new ExposingPrincipalSecretsGenerator("REALM");
    Assertions.assertInstanceOf(
        EnvVariablePrincipalSecretsGenerator.class, hasVars.seeDelegate("PRINCIPAL"));
  }
}
