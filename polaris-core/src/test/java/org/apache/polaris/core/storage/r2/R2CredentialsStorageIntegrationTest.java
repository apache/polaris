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
package org.apache.polaris.core.storage.r2;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.storage.CredentialVendingContext;
import org.apache.polaris.core.storage.LocationGrant;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheConfig;
import org.junit.jupiter.api.Test;

class R2CredentialsStorageIntegrationTest {

  private static final String ACCOUNT = "0123456789abcdef0123456789abcdef";
  private static final RealmConfig REALM_CONFIG = new RealmConfigImpl(EMPTY_CONFIG, () -> "realm");
  private static final Clock CLOCK =
      Clock.fixed(Instant.now().truncatedTo(ChronoUnit.SECONDS), ZoneOffset.UTC);
  private static final R2StorageConfigurationInfo CONFIG =
      R2StorageConfigurationInfo.builder()
          .accountId(ACCOUNT)
          .storageName("primary")
          .addAllowedLocations("s3://bucket/wh/")
          .build();
  private static final R2ParentTokenResolver RESOLVER =
      name ->
          "primary".equals(name) ? Optional.of(new R2ParentToken("pk", "ps")) : Optional.empty();
  private static final List<LocationGrant> GRANTS =
      List.of(new LocationGrant(Set.of("s3://bucket/wh/db/t/"), Set.of(PolarisStorageActions.ALL)));

  @Test
  void vendsWithoutCache() {
    R2CredentialsStorageIntegration integration =
        new R2CredentialsStorageIntegration(RESOLVER, CLOCK, null, CONFIG, REALM_CONFIG);
    StorageAccessConfig cfg =
        integration.getStorageAccessConfig(
            GRANTS, Optional.empty(), CredentialVendingContext.empty());
    assertThat(cfg.credentials())
        .containsEntry(StorageAccessProperty.R2_KEY_ID.getPropertyName(), "pk");
    assertThat(cfg.extraProperties())
        .containsEntry("s3.endpoint", "https://" + ACCOUNT + ".r2.cloudflarestorage.com");
  }

  @Test
  void secondCallIsServedFromCache() {
    StorageCredentialCacheConfig cacheConfig = () -> 10_000;
    StorageCredentialCache cache = new StorageCredentialCache(cacheConfig);
    R2CredentialsStorageIntegration integration =
        new R2CredentialsStorageIntegration(RESOLVER, CLOCK, cache, CONFIG, REALM_CONFIG);
    StorageAccessConfig first =
        integration.getStorageAccessConfig(
            GRANTS, Optional.empty(), CredentialVendingContext.empty());
    StorageAccessConfig second =
        integration.getStorageAccessConfig(
            GRANTS, Optional.empty(), CredentialVendingContext.empty());
    assertThat(second).isSameAs(first);
    assertThat(cache.getEstimatedSize()).isEqualTo(1);
  }

  @Test
  void missingParentTokenIsAnIllegalArgument() {
    R2StorageConfigurationInfo unconfigured =
        R2StorageConfigurationInfo.builder()
            .accountId(ACCOUNT)
            .storageName("unknown")
            .addAllowedLocations("s3://bucket/wh/")
            .build();
    R2CredentialsStorageIntegration integration =
        new R2CredentialsStorageIntegration(RESOLVER, CLOCK, null, unconfigured, REALM_CONFIG);
    assertThatThrownBy(
            () ->
                integration.getStorageAccessConfig(
                    GRANTS, Optional.empty(), CredentialVendingContext.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unknown");
  }

  @Test
  void multiBucketGrantsAreRejected() {
    R2CredentialsStorageIntegration integration =
        new R2CredentialsStorageIntegration(RESOLVER, CLOCK, null, CONFIG, REALM_CONFIG);
    List<LocationGrant> grants =
        List.of(
            new LocationGrant(
                Set.of("s3://bucket/a/", "s3://other/b/"), Set.of(PolarisStorageActions.READ)));
    assertThatThrownBy(
            () ->
                integration.getStorageAccessConfig(
                    grants, Optional.empty(), CredentialVendingContext.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("one bucket");
  }
}
