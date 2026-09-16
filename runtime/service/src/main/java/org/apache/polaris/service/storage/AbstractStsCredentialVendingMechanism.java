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

import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsCredentialsStorageIntegration;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.aws.S3CredentialVendingMechanism;
import org.apache.polaris.core.storage.aws.StsClientProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/** AWS STS AssumeRole against the catalog's role; the body shared by the STS and DEFAULT beans. */
public abstract class AbstractStsCredentialVendingMechanism
    implements S3CredentialVendingMechanism {

  private final StsClientProvider stsClientProvider;
  private final BiFunction<
          AwsStorageConfigurationInfo, RealmConfig, Optional<AwsCredentialsProvider>>
      credentialsResolver;
  private final StorageCredentialCache cache;

  /**
   * For the client proxy ArC generates for each normal-scoped leaf bean: the proxy subclass needs a
   * no-args constructor, and ArC synthesizes one for the leaf only when its superclass has one. A
   * working instance always comes from one of the constructors below.
   */
  protected AbstractStsCredentialVendingMechanism() {
    this.stsClientProvider = null;
    this.credentialsResolver = null;
    this.cache = null;
  }

  protected AbstractStsCredentialVendingMechanism(
      StorageConfiguration storageConfiguration,
      StsClientProvider stsClientProvider,
      StorageCredentialCache cache) {
    this.stsClientProvider = stsClientProvider;
    this.cache = cache;
    this.credentialsResolver =
        (config, realmConfig) -> {
          if (realmConfig.getConfig(FeatureConfiguration.RESOLVE_CREDENTIALS_BY_STORAGE_NAME)) {
            return Optional.of(storageConfiguration.stsCredentials(config.getStorageName()));
          }
          return Optional.of(storageConfiguration.stsCredentials());
        };
  }

  /** Test constructor: a fixed credentials provider, the realm config is never consulted. */
  protected AbstractStsCredentialVendingMechanism(
      StsClientProvider stsClientProvider,
      Optional<AwsCredentialsProvider> stsCredentials,
      StorageCredentialCache cache) {
    this.stsClientProvider = stsClientProvider;
    this.cache = cache;
    this.credentialsResolver = (config, realmConfig) -> stsCredentials;
  }

  @Override
  public PolarisStorageIntegration integrationFor(
      AwsStorageConfigurationInfo storageConfig, RealmConfig realmConfig) {
    return new AwsCredentialsStorageIntegration(
        stsClientProvider,
        config -> credentialsResolver.apply(config, realmConfig),
        cache,
        storageConfig,
        realmConfig);
  }
}
