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
package org.apache.polaris.service.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.azure.identity.ClientSecretCredentialBuilder;
import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.HashMap;
import java.util.Map;
import org.apache.polaris.service.storage.StorageConfiguration.AzureNamedCredential;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class AzureNamedCredentialsTest {

  private static final String TENANT = "11111111-1111-1111-1111-111111111111";
  private static final String SECRET_A = "secret-value-of-team-a";
  private static final String SECRET_B = "secret-value-of-team-b";

  private static StorageConfiguration configFrom(Map<String, String> properties) {
    return configFrom(properties, Map.of());
  }

  private static StorageConfiguration configFrom(
      Map<String, String> properties, Map<String, String> environment) {
    return new SmallRyeConfigBuilder()
        .withMapping(StorageConfiguration.class)
        .withSources(new PropertiesConfigSource(properties, "test", 100))
        .withSources(new EnvConfigSource(environment, 300))
        .build()
        .getConfigMapping(StorageConfiguration.class);
  }

  private static Map<String, String> properties(String name, String clientSecret) {
    return Map.of(
        "polaris.storage.azure." + name + ".tenant-id", TENANT,
        "polaris.storage.azure." + name + ".client-id", "app-" + name,
        "polaris.storage.azure." + name + ".client-secret", clientSecret);
  }

  private static Map<String, String> twoNames() {
    Map<String, String> properties = new HashMap<>(properties("team-a", SECRET_A));
    properties.putAll(properties("team-b", SECRET_B));
    return properties;
  }

  /** Builds the credential and returns the builder that built it, to check what it was given. */
  private static ClientSecretCredentialBuilder builderOf(AzureNamedCredential named) {
    try (MockedConstruction<ClientSecretCredentialBuilder> builders =
        Mockito.mockConstruction(
            ClientSecretCredentialBuilder.class,
            Mockito.withSettings().defaultAnswer(Answers.RETURNS_SELF))) {
      // The mocked builder builds nothing; only the values it was given matter here.
      assertThat(named.credential().get()).isNull();
      assertThat(builders.constructed()).hasSize(1);
      return builders.constructed().get(0);
    }
  }

  @Test
  void eachNameUsesItsOwnServicePrincipal() {
    Map<String, AzureNamedCredential> named = configFrom(twoNames()).azureNamedCredentials();

    assertThat(named).containsOnlyKeys("team-a", "team-b");
    assertThat(named.get("team-a").tenantId()).isEqualTo(TENANT);
    ClientSecretCredentialBuilder builderA = builderOf(named.get("team-a"));
    verify(builderA).tenantId(TENANT);
    verify(builderA).clientId("app-team-a");
    verify(builderA).clientSecret(SECRET_A);
    ClientSecretCredentialBuilder builderB = builderOf(named.get("team-b"));
    verify(builderB).clientId("app-team-b");
    verify(builderB).clientSecret(SECRET_B);
  }

  @Test
  void nameIsBuiltOnceSoItsTokenCacheIsShared() {
    AzureNamedCredential named =
        configFrom(properties("team-a", SECRET_A)).azureNamedCredentials().get("team-a");

    assertThat(named.credential().get()).isNotNull().isSameAs(named.credential().get());
  }

  @Test
  void badEntryFailsOnlyItsOwnNameAndKeepsTheSecretOutOfTheError() {
    Map<String, String> properties = twoNames();
    properties.put("polaris.storage.azure.team-a.tenant-id", "not/a/tenant");
    Map<String, AzureNamedCredential> named = configFrom(properties).azureNamedCredentials();

    assertThatThrownBy(() -> named.get("team-a").credential().get())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("team-a")
        .hasMessageNotContaining(SECRET_A)
        .hasMessageNotContaining("not/a/tenant")
        .hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(named.get("team-b").credential().get()).isNotNull();
  }

  @Test
  void incompleteEntryFailsAtStartupNamingThePropertyButNoValues() {
    assertThatThrownBy(
            () ->
                configFrom(
                    Map.of(
                        "polaris.storage.azure.team-a.tenant-id", TENANT,
                        "polaris.storage.azure.team-a.client-secret", SECRET_A)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("polaris.storage.azure.team-a.client-id")
        .hasMessageNotContaining(SECRET_A);
  }

  @Test
  void environmentVariableSuppliesTheSecretOfANameDefinedByProperties() {
    Map<String, AzureNamedCredential> named =
        configFrom(
                Map.of(
                    "polaris.storage.azure.team-a.tenant-id",
                    TENANT,
                    "polaris.storage.azure.team-a.client-id",
                    "app-team-a"),
                Map.of("POLARIS_STORAGE_AZURE_TEAM_A_CLIENT_SECRET", SECRET_A))
            .azureNamedCredentials();

    verify(builderOf(named.get("team-a"))).clientSecret(SECRET_A);
  }
}
