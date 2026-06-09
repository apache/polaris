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
package org.apache.polaris.core.storage.aws;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.StorageAccessConfig;
import org.apache.polaris.core.storage.cache.StorageCredentialCacheKey;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.model.Tag;

/**
 * Cache key for vended AWS credentials. Data fields drive equality (cache identity); auxiliary
 * fields carry the app-scoped deps that {@link #load()} needs to mint a fresh credential on miss.
 */
@PolarisImmutable
public interface AwsStorageCredentialCacheKey extends StorageCredentialCacheKey {

  // ---- data fields: part of equals/hashCode ----

  @Value.Parameter(order = 1)
  String realmId();

  @Value.Parameter(order = 2)
  AwsStorageConfigurationInfo storageConfig();

  @Value.Parameter(order = 3)
  Set<String> allowedReadLocations();

  @Value.Parameter(order = 4)
  Set<String> allowedListLocations();

  @Value.Parameter(order = 5)
  Set<String> allowedWriteLocations();

  @Value.Parameter(order = 6)
  Optional<String> refreshCredentialsEndpoint();

  @Value.Parameter(order = 7)
  String roleSessionName();

  @Value.Parameter(order = 8)
  List<Tag> sessionTags();

  // ---- aux: app-scoped invariants, excluded from equals/hashCode ----

  @Value.Parameter(order = 9)
  @Value.Auxiliary
  StsClientProvider stsClientProvider();

  @Value.Parameter(order = 10)
  @Value.Auxiliary
  Function<AwsStorageConfigurationInfo, Optional<AwsCredentialsProvider>> credentialsResolver();

  @Override
  @Value.Parameter(order = 11)
  @Value.Auxiliary
  RealmConfig realmConfig();

  @Override
  default StorageAccessConfig load() {
    return AwsCredentialsStorageIntegration.compute(this);
  }

  static AwsStorageCredentialCacheKey of(
      String realmId,
      AwsStorageConfigurationInfo storageConfig,
      Set<String> allowedReadLocations,
      Set<String> allowedListLocations,
      Set<String> allowedWriteLocations,
      Optional<String> refreshCredentialsEndpoint,
      String roleSessionName,
      List<Tag> sessionTags,
      StsClientProvider stsClientProvider,
      Function<AwsStorageConfigurationInfo, Optional<AwsCredentialsProvider>> credentialsResolver,
      RealmConfig realmConfig) {
    return ImmutableAwsStorageCredentialCacheKey.of(
        realmId,
        storageConfig,
        allowedReadLocations,
        allowedListLocations,
        allowedWriteLocations,
        refreshCredentialsEndpoint,
        roleSessionName,
        sessionTags,
        stsClientProvider,
        credentialsResolver,
        realmConfig);
  }
}
