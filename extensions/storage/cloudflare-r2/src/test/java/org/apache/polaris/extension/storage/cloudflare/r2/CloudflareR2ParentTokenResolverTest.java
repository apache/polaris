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

import static org.apache.polaris.core.config.FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.Map;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.junit.jupiter.api.Test;

class CloudflareR2ParentTokenResolverTest {

  private static final RealmContext REALM = () -> "test-realm";
  private static final String LOCATION = "s3://test-bucket/warehouse/";
  private static final CloudflareR2ParentToken DEFAULT_TOKEN =
      new CloudflareR2ParentToken("test-r2-access-key", "test-r2-secret-key");
  private static final CloudflareR2ParentToken NAMED_TOKEN =
      new CloudflareR2ParentToken("test-r2-named-access-key", "test-r2-named-secret-key");

  private static RealmConfig realmConfig(Map<String, Object> overrides) {
    return new RealmConfigImpl((rc, name) -> overrides.get(name), REALM);
  }

  private static AwsStorageConfigurationInfo configWithoutStorageName() {
    return AwsStorageConfigurationInfo.builder().addAllowedLocation(LOCATION).build();
  }

  private static AwsStorageConfigurationInfo configWithStorageName(String storageName) {
    return AwsStorageConfigurationInfo.builder()
        .addAllowedLocation(LOCATION)
        .storageName(storageName)
        .build();
  }

  /**
   * The resolver the server builds, from a mapping that holds the default token and the named token
   * for {@code research}. The values are dummy tokens, not secrets.
   */
  private static final CloudflareR2ParentTokenResolver RESOLVER =
      new CloudflareR2ParentTokenResolverImpl(
          new SmallRyeConfigBuilder()
              .withMapping(CloudflareR2Configuration.class)
              .withSources(
                  new PropertiesConfigSource(
                      Map.of(
                          "polaris.storage.cloudflare-r2.access-key", DEFAULT_TOKEN.accessKeyId(),
                          "polaris.storage.cloudflare-r2.secret-key",
                              DEFAULT_TOKEN.secretAccessKey(),
                          "polaris.storage.cloudflare-r2.research.access-key",
                              NAMED_TOKEN.accessKeyId(),
                          "polaris.storage.cloudflare-r2.research.secret-key",
                              NAMED_TOKEN.secretAccessKey()),
                      "test-properties"))
              .build()
              .getConfigMapping(CloudflareR2Configuration.class));

  @Test
  void effectiveStorageNameIsTheStorageNameWhenTheFlagIsTrue() {
    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true));
    assertThat(
            CloudflareR2ParentTokenResolver.effectiveStorageName(
                realmConfig, configWithStorageName("research")))
        .isEqualTo("research");
  }

  @Test
  void effectiveStorageNameIsNullWhenTheFlagIsFalse() {
    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false));
    assertThat(
            CloudflareR2ParentTokenResolver.effectiveStorageName(
                realmConfig, configWithStorageName("research")))
        .isNull();
  }

  @Test
  void effectiveStorageNameIsNullWhenTheFlagIsUnset() {
    assertThat(
            CloudflareR2ParentTokenResolver.effectiveStorageName(
                realmConfig(Map.of()), configWithStorageName("research")))
        .isNull();
  }

  @Test
  void effectiveStorageNameIsNullWhenTheFlagIsTrueAndTheConfigHasNoStorageName() {
    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true));
    assertThat(
            CloudflareR2ParentTokenResolver.effectiveStorageName(
                realmConfig, configWithoutStorageName()))
        .isNull();
  }

  @Test
  void aTrueFlagSelectsTheNamedToken() {
    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), true));
    String name =
        CloudflareR2ParentTokenResolver.effectiveStorageName(
            realmConfig, configWithStorageName("research"));
    assertThat(RESOLVER.resolve(name)).contains(NAMED_TOKEN);
  }

  @Test
  void aFalseFlagSelectsTheDefaultTokenEvenForANamedCatalog() {
    RealmConfig realmConfig = realmConfig(Map.of(RESOLVE_CREDENTIALS_BY_STORAGE_NAME.key(), false));
    String name =
        CloudflareR2ParentTokenResolver.effectiveStorageName(
            realmConfig, configWithStorageName("research"));
    assertThat(RESOLVER.resolve(name)).contains(DEFAULT_TOKEN);
  }
}
