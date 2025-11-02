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
package org.apache.polaris.core.storage;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class InMemoryStorageIntegrationTest {

  private static final RealmContext REALM_CONTEXT = () -> "realm";

  @ParameterizedTest
  @CsvSource({"s3,s3", "s3,s3a", "s3a,s3", "s3a,s3a"})
  public void testValidateAccessToLocations(String allowedScheme, String locationScheme) {
    RealmConfig realmConfig =
        new RealmConfigImpl(new MockedConfigurationStore(Map.of()), REALM_CONTEXT);
    MockInMemoryStorageIntegration storage = new MockInMemoryStorageIntegration();
    Map<String, Map<PolarisStorageActions, PolarisStorageIntegration.ValidationResult>> result =
        storage.validateAccessToLocations(
            realmConfig,
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocations(
                    allowedScheme + "://bucket/path/to/warehouse",
                    allowedScheme + "://bucket/anotherpath/to/warehouse",
                    allowedScheme + "://bucket2/warehouse/")
                .roleARN("arn:aws:iam::012345678901:role/jdoe")
                .region("us-east-2")
                .build(),
            Set.of(PolarisStorageActions.READ),
            Set.of(
                locationScheme + "://bucket/path/to/warehouse/namespace/table",
                locationScheme + "://bucket2/warehouse",
                locationScheme + "://arandombucket/path/to/warehouse/namespace/table"));
    Assertions.assertThat(result)
        .hasSize(3)
        .containsEntry(
            locationScheme + "://bucket/path/to/warehouse/namespace/table",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(true, "")))
        .containsEntry(
            locationScheme + "://bucket2/warehouse",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(true, "")))
        .containsEntry(
            locationScheme + "://arandombucket/path/to/warehouse/namespace/table",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(false, "")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a"})
  public void testValidateAccessToLocationsWithWildcard(String s3Scheme) {
    MockInMemoryStorageIntegration storage = new MockInMemoryStorageIntegration();
    Map<String, Object> config = Map.of("ALLOW_WILDCARD_LOCATION", true);
    RealmConfig realmConfig =
        new RealmConfigImpl(new MockedConfigurationStore(config), REALM_CONTEXT);
    Map<String, Map<PolarisStorageActions, PolarisStorageIntegration.ValidationResult>> result =
        storage.validateAccessToLocations(
            realmConfig,
            FileStorageConfigurationInfo.builder().addAllowedLocations("file://", "*").build(),
            Set.of(PolarisStorageActions.READ),
            Set.of(
                s3Scheme + "://bucket/path/to/warehouse/namespace/table",
                "file:///etc/passwd",
                "a/relative/subdirectory"));
    Assertions.assertThat(result)
        .hasSize(3)
        .hasEntrySatisfying(
            s3Scheme + "://bucket/path/to/warehouse/namespace/table",
            val ->
                Assertions.assertThat(val)
                    .hasSize(1)
                    .containsKey(PolarisStorageActions.READ)
                    .extractingByKey(PolarisStorageActions.READ)
                    .returns(true, PolarisStorageIntegration.ValidationResult::isSuccess))
        .hasEntrySatisfying(
            "file:///etc/passwd",
            val ->
                Assertions.assertThat(val)
                    .hasSize(1)
                    .containsKey(PolarisStorageActions.READ)
                    .extractingByKey(PolarisStorageActions.READ)
                    .returns(true, PolarisStorageIntegration.ValidationResult::isSuccess))
        .hasEntrySatisfying(
            "a/relative/subdirectory",
            val ->
                Assertions.assertThat(val)
                    .hasSize(1)
                    .containsKey(PolarisStorageActions.READ)
                    .extractingByKey(PolarisStorageActions.READ)
                    .returns(true, PolarisStorageIntegration.ValidationResult::isSuccess));
  }

  @Test
  public void testValidateAccessToLocationsNoAllowedLocations() {
    MockInMemoryStorageIntegration storage = new MockInMemoryStorageIntegration();
    RealmConfig realmConfig =
        new RealmConfigImpl(new MockedConfigurationStore(Map.of()), REALM_CONTEXT);
    Map<String, Map<PolarisStorageActions, PolarisStorageIntegration.ValidationResult>> result =
        storage.validateAccessToLocations(
            realmConfig,
            AwsStorageConfigurationInfo.builder()
                .roleARN("arn:aws:iam::012345678901:role/jdoe")
                .region("us-east-2")
                .build(),
            Set.of(PolarisStorageActions.READ),
            Set.of(
                "s3://bucket/path/to/warehouse/namespace/table",
                "s3://bucket2/warehouse/namespace/table",
                "s3://arandombucket/path/to/warehouse/namespace/table"));
    Assertions.assertThat(result)
        .hasSize(3)
        .containsEntry(
            "s3://bucket/path/to/warehouse/namespace/table",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(false, "")))
        .containsEntry(
            "s3://bucket2/warehouse/namespace/table",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(false, "")))
        .containsEntry(
            "s3://arandombucket/path/to/warehouse/namespace/table",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(false, "")));
  }

  @Test
  public void testValidateAccessToLocationsWithPrefixOfAllowedLocation() {
    MockInMemoryStorageIntegration storage = new MockInMemoryStorageIntegration();
    RealmConfig realmConfig =
        new RealmConfigImpl(new MockedConfigurationStore(Map.of()), REALM_CONTEXT);
    Map<String, Map<PolarisStorageActions, PolarisStorageIntegration.ValidationResult>> result =
        storage.validateAccessToLocations(
            realmConfig,
            AwsStorageConfigurationInfo.builder()
                .addAllowedLocation("s3://bucket/path/to/warehouse")
                .roleARN("arn:aws:iam::012345678901:role/jdoe")
                .region("us-east-2")
                .build(),
            Set.of(PolarisStorageActions.READ),
            // trying to read a prefix under the allowed location
            Set.of("s3://bucket/path/to"));
    Assertions.assertThat(result)
        .hasSize(1)
        .containsEntry(
            "s3://bucket/path/to",
            Map.of(
                PolarisStorageActions.READ,
                new PolarisStorageIntegration.ValidationResult(false, "")));
  }

  private static final class MockInMemoryStorageIntegration
      extends InMemoryStorageIntegration<PolarisStorageConfigurationInfo> {
    public MockInMemoryStorageIntegration() {
      super(
          Mockito.mock(PolarisStorageConfigurationInfo.class),
          MockInMemoryStorageIntegration.class.getName());
    }

    @Override
    public StorageAccessConfig getSubscopedCreds(
        @Nonnull RealmConfig realmConfig,
        boolean allowListOperation,
        @Nonnull Set<String> allowedReadLocations,
        @Nonnull Set<String> allowedWriteLocations,
        Optional<String> refreshCredentialsEndpoint) {
      return null;
    }
  }

  private static class MockedConfigurationStore implements PolarisConfigurationStore {
    private final Map<String, Object> defaults;

    public MockedConfigurationStore(Map<String, Object> defaults) {
      this.defaults = Map.copyOf(defaults);
    }

    @Override
    public <T> @Nullable T getConfiguration(@Nonnull RealmContext realmContext, String configName) {
      @SuppressWarnings("unchecked")
      T confgValue = (T) defaults.get(configName);
      return confgValue;
    }
  }
}
