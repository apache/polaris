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
package org.apache.polaris.extension.storage.cloudflare.r2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.smallrye.config.ConfigValidationException;
import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Builds a real {@code SmallRyeConfig} over {@link CloudflareR2Configuration}, so the tests cover
 * the key names, the map of named tokens, and the required properties of a named token. The values
 * are dummy tokens, not secrets.
 */
class CloudflareR2ConfigurationTest {

  private static final String PREFIX = "polaris.storage.cloudflare-r2.";

  private static final String ACCESS_KEY = "test-r2-access-key";
  private static final String SECRET_KEY = "test-r2-secret-key";
  private static final String PROD_ACCESS_KEY = "test-r2-prod-access-key";
  private static final String PROD_SECRET_KEY = "test-r2-prod-secret-key";
  private static final String TEAM_A_ACCESS_KEY = "test-r2-team-a-access-key";
  private static final String TEAM_A_SECRET_KEY = "test-r2-team-a-secret-key";

  private static SmallRyeConfigBuilder builder() {
    return new SmallRyeConfigBuilder().withMapping(CloudflareR2Configuration.class);
  }

  /** Maps properties, as they arrive from {@code application.properties} or {@code -D}. */
  private static CloudflareR2Configuration mapProperties(Map<String, String> properties) {
    return builder()
        .withSources(new PropertiesConfigSource(properties, "test-properties"))
        .build()
        .getConfigMapping(CloudflareR2Configuration.class);
  }

  /** Maps environment variables, through the same source type the server uses for them. */
  private static CloudflareR2Configuration mapEnvironment(Map<String, String> environment) {
    return builder()
        .withSources(new EnvConfigSource(environment, 300))
        .build()
        .getConfigMapping(CloudflareR2Configuration.class);
  }

  @Test
  void noKeysGiveNoDefaultTokenAndNoNamedTokens() {
    CloudflareR2Configuration mapping = mapProperties(Map.of());

    assertThat(mapping.accessKey()).isEmpty();
    assertThat(mapping.secretKey()).isEmpty();
    assertThat(mapping.storages()).isEmpty();
  }

  @Test
  void defaultPairIsRead() {
    CloudflareR2Configuration mapping =
        mapProperties(Map.of(PREFIX + "access-key", ACCESS_KEY, PREFIX + "secret-key", SECRET_KEY));

    assertThat(mapping.accessKey()).contains(ACCESS_KEY);
    assertThat(mapping.secretKey()).contains(SECRET_KEY);
    assertThat(mapping.storages()).isEmpty();
  }

  @Test
  void accessKeyAloneBuildsAndLeavesTheSecretKeyEmpty() {
    CloudflareR2Configuration mapping = mapProperties(Map.of(PREFIX + "access-key", ACCESS_KEY));

    assertThat(mapping.accessKey()).contains(ACCESS_KEY);
    assertThat(mapping.secretKey()).isEmpty();
    assertThat(mapping.storages()).isEmpty();
  }

  @Test
  void secretKeyAloneBuildsAndLeavesTheAccessKeyEmpty() {
    CloudflareR2Configuration mapping = mapProperties(Map.of(PREFIX + "secret-key", SECRET_KEY));

    assertThat(mapping.accessKey()).isEmpty();
    assertThat(mapping.secretKey()).contains(SECRET_KEY);
    assertThat(mapping.storages()).isEmpty();
  }

  @Test
  void namedPairIsRead() {
    CloudflareR2Configuration mapping =
        mapProperties(
            Map.of(
                PREFIX + "prod.access-key", PROD_ACCESS_KEY,
                PREFIX + "prod.secret-key", PROD_SECRET_KEY));

    assertThat(mapping.accessKey()).isEmpty();
    assertThat(mapping.secretKey()).isEmpty();
    assertThat(mapping.storages()).containsOnlyKeys("prod");
    CloudflareR2Configuration.NamedToken prod = mapping.storages().get("prod");
    assertThat(prod.accessKey()).isEqualTo(PROD_ACCESS_KEY);
    assertThat(prod.secretKey()).isEqualTo(PROD_SECRET_KEY);
  }

  @Test
  void defaultPairAndNamedPairsStaySeparate() {
    CloudflareR2Configuration mapping =
        mapProperties(
            Map.of(
                PREFIX + "access-key", ACCESS_KEY,
                PREFIX + "secret-key", SECRET_KEY,
                PREFIX + "prod.access-key", PROD_ACCESS_KEY,
                PREFIX + "prod.secret-key", PROD_SECRET_KEY,
                PREFIX + "team-a.access-key", "test-r2-team-a-access-key",
                PREFIX + "team-a.secret-key", "test-r2-team-a-secret-key"));

    assertThat(mapping.accessKey()).contains(ACCESS_KEY);
    assertThat(mapping.secretKey()).contains(SECRET_KEY);
    assertThat(mapping.storages()).containsOnlyKeys("prod", "team-a");
    assertThat(mapping.storages().get("prod").accessKey()).isEqualTo(PROD_ACCESS_KEY);
    assertThat(mapping.storages().get("team-a").secretKey()).isEqualTo("test-r2-team-a-secret-key");
  }

  @Test
  void namedPairWithoutItsSecretKeyFailsToBuild() {
    assertThatThrownBy(() -> mapProperties(Map.of(PREFIX + "prod.access-key", PROD_ACCESS_KEY)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining(PREFIX + "prod.secret-key");
  }

  @Test
  void namedPairWithoutItsAccessKeyFailsToBuild() {
    assertThatThrownBy(() -> mapProperties(Map.of(PREFIX + "prod.secret-key", PROD_SECRET_KEY)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining(PREFIX + "prod.access-key");
  }

  @Test
  void defaultPairIsReadFromEnvironmentStyleNames() {
    CloudflareR2Configuration mapping =
        mapEnvironment(
            Map.of(
                "POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY", ACCESS_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY", SECRET_KEY));

    assertThat(mapping.accessKey()).contains(ACCESS_KEY);
    assertThat(mapping.secretKey()).contains(SECRET_KEY);
    assertThat(mapping.storages()).isEmpty();
  }

  /**
   * Named tokens from environment variables alone. The map key of a named token comes from the
   * environment name, in lower case: {@code PROD} gives {@code prod}. This test uses a storage name
   * of one word. {@link #aStorageNameWithASeparatorIsNotReadFromEnvironmentStyleNames} covers a
   * name that holds a separator.
   */
  @Test
  void namedPairIsReadFromEnvironmentStyleNames() {
    CloudflareR2Configuration mapping =
        mapEnvironment(
            Map.of(
                "POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY", ACCESS_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY", SECRET_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_PROD_ACCESS_KEY", PROD_ACCESS_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_PROD_SECRET_KEY", PROD_SECRET_KEY));

    assertThat(mapping.accessKey()).contains(ACCESS_KEY);
    assertThat(mapping.secretKey()).contains(SECRET_KEY);
    assertThat(mapping.storages()).containsOnlyKeys("prod");
    assertThat(mapping.storages().get("prod").accessKey()).isEqualTo(PROD_ACCESS_KEY);
    assertThat(mapping.storages().get("prod").secretKey()).isEqualTo(PROD_SECRET_KEY);
  }

  /**
   * An environment name with a separator in the storage name sets no named token, and the mapping
   * still builds. The default pair is not affected. The configuration page states this limit.
   */
  @Test
  void aStorageNameWithASeparatorIsNotReadFromEnvironmentStyleNames() {
    CloudflareR2Configuration mapping =
        mapEnvironment(
            Map.of(
                "POLARIS_STORAGE_CLOUDFLARE_R2_ACCESS_KEY", ACCESS_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_SECRET_KEY", SECRET_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_TEAM_A_ACCESS_KEY", TEAM_A_ACCESS_KEY,
                "POLARIS_STORAGE_CLOUDFLARE_R2_TEAM_A_SECRET_KEY", TEAM_A_SECRET_KEY));

    assertThat(mapping.accessKey()).contains(ACCESS_KEY);
    assertThat(mapping.secretKey()).contains(SECRET_KEY);
    assertThat(mapping.storages()).isEmpty();
  }

  /** A storage name that holds a dot needs quotes in the property name. */
  @Test
  void aDottedStorageNameIsReadOnlyFromAQuotedPropertyName() {
    CloudflareR2Configuration mapping =
        mapProperties(
            Map.of(
                PREFIX + "\"prod.eu\".access-key", PROD_ACCESS_KEY,
                PREFIX + "\"prod.eu\".secret-key", PROD_SECRET_KEY));

    assertThat(mapping.storages()).containsOnlyKeys("prod.eu");
    assertThat(mapping.storages().get("prod.eu").accessKey()).isEqualTo(PROD_ACCESS_KEY);
    assertThat(mapping.storages().get("prod.eu").secretKey()).isEqualTo(PROD_SECRET_KEY);

    assertThatThrownBy(
            () ->
                mapProperties(
                    Map.of(
                        PREFIX + "prod.eu.access-key", PROD_ACCESS_KEY,
                        PREFIX + "prod.eu.secret-key", PROD_SECRET_KEY)))
        .isInstanceOf(ConfigValidationException.class)
        .hasMessageContaining("SRCFG00050");
  }
}
