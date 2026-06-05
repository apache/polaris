/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may apply this License at
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
import static org.mockito.Mockito.mock;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.storage.CachingStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheConfig;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;

class PolarisStorageIntegrationProviderImplTest {

  private static final AwsStorageConfigurationInfo BASE_AWS_CONFIG =
      AwsStorageConfigurationInfo.builder()
          .addAllowedLocations("s3://foo/bar")
          .roleARN("arn:aws:iam::123456789012:role/polaris-test")
          .region("us-east-1")
          .storageName("default")
          .build();

  @Test
  void noOverrideReturnsBaseStorageName() {
    PolarisStorageIntegrationProviderImpl provider = newProvider();

    PolarisStorageIntegration integration =
        provider.getStorageIntegration(List.of(catalogEntity()));

    assertThat(integration).isInstanceOf(CachingStorageIntegration.class);
    PolarisStorageConfigurationInfo bound =
        ((CachingStorageIntegration<?>) integration).storageConfig();
    assertThat(bound.getStorageName()).isEqualTo("default");
  }

  @Test
  void namespaceOverrideAppliedToBoundConfig() {
    PolarisStorageIntegrationProviderImpl provider = newProvider();

    List<PolarisEntity> path = List.of(catalogEntity(), namespaceEntityWithOverride("team-a"));

    PolarisStorageIntegration integration = provider.getStorageIntegration(path);

    assertThat(integration).isInstanceOf(CachingStorageIntegration.class);
    PolarisStorageConfigurationInfo bound =
        ((CachingStorageIntegration<?>) integration).storageConfig();
    assertThat(bound.getStorageName()).isEqualTo("team-a");
    // Other fields preserved.
    assertThat(((AwsStorageConfigurationInfo) bound).getRoleARN())
        .isEqualTo(BASE_AWS_CONFIG.getRoleARN());
  }

  @Test
  void leafOverrideBeatsAncestorOverride() {
    PolarisStorageIntegrationProviderImpl provider = newProvider();

    List<PolarisEntity> path =
        List.of(
            catalogEntity(),
            namespaceEntityWithOverride("team-namespace"),
            tableEntityWithOverride("table-special"));

    PolarisStorageIntegration integration = provider.getStorageIntegration(path);

    PolarisStorageConfigurationInfo bound =
        ((CachingStorageIntegration<?>) integration).storageConfig();
    assertThat(bound.getStorageName()).isEqualTo("table-special");
  }

  @Test
  void distinctOverridesProduceDistinctBoundConfigsForCacheSegregation() {
    PolarisStorageIntegrationProviderImpl provider = newProvider();

    PolarisStorageIntegration teamA =
        provider.getStorageIntegration(
            List.of(catalogEntity(), namespaceEntityWithOverride("team-a")));
    PolarisStorageIntegration teamB =
        provider.getStorageIntegration(
            List.of(catalogEntity(), namespaceEntityWithOverride("team-b")));

    PolarisStorageConfigurationInfo configA =
        ((CachingStorageIntegration<?>) teamA).storageConfig();
    PolarisStorageConfigurationInfo configB =
        ((CachingStorageIntegration<?>) teamB).storageConfig();

    // The bound configs differ only in storageName, but Immutables-generated equals/hashCode
    // includes that field, so AwsStorageCredentialCacheKey (which has storageConfig as a value
    // field) will produce distinct keys for these two paths.
    assertThat(configA).isNotEqualTo(configB);
    assertThat(configA.hashCode()).isNotEqualTo(configB.hashCode());
    assertThat(configA.getStorageName()).isEqualTo("team-a");
    assertThat(configB.getStorageName()).isEqualTo("team-b");
  }

  @Test
  void noBaseConfigReturnsNullEvenWithOverride() {
    PolarisStorageIntegrationProviderImpl provider = newProvider();

    // namespace-only path with no catalog config
    PolarisStorageIntegration integration =
        provider.getStorageIntegration(List.of(namespaceEntityWithOverride("orphan")));

    assertThat(integration).isNull();
  }

  // --- helpers -------------------------------------------------------------

  private PolarisStorageIntegrationProviderImpl newProvider() {
    StsClient stsClient = mock(StsClient.class);
    StorageCredentialCacheConfig cacheConfig = () -> 1024;
    StorageCredentialCache cache = new StorageCredentialCache(cacheConfig);
    RealmConfig realmConfig = mock(RealmConfig.class);
    return new PolarisStorageIntegrationProviderImpl(
        (destination) -> stsClient,
        Optional.empty(),
        () -> GoogleCredentials.create(new AccessToken("token", new Date())),
        cache,
        realmConfig,
        new PolarisDefaultDiagServiceImpl());
  }

  private PolarisEntity catalogEntity() {
    Map<String, String> internal = new HashMap<>();
    internal.put(
        PolarisEntityConstants.getStorageConfigInfoPropertyName(), BASE_AWS_CONFIG.serialize());
    return entity(PolarisEntityType.CATALOG, "cat", internal);
  }

  private PolarisEntity namespaceEntityWithOverride(String name) {
    return entity(
        PolarisEntityType.NAMESPACE,
        "ns",
        Map.of(PolarisEntityConstants.getStorageNameOverridePropertyName(), name));
  }

  private PolarisEntity tableEntityWithOverride(String name) {
    return entity(
        PolarisEntityType.TABLE_LIKE,
        "tbl",
        Map.of(PolarisEntityConstants.getStorageNameOverridePropertyName(), name));
  }

  private PolarisEntity entity(
      PolarisEntityType type, String name, Map<String, String> internalProps) {
    PolarisBaseEntity base =
        new PolarisBaseEntity.Builder()
            .catalogId(1L)
            .id(System.nanoTime())
            .typeCode(type.getCode())
            .subTypeCode(PolarisEntitySubType.NULL_SUBTYPE.getCode())
            .parentId(0L)
            .name(name)
            .internalPropertiesAsMap(internalProps)
            .build();
    return new PolarisEntity(base);
  }
}
