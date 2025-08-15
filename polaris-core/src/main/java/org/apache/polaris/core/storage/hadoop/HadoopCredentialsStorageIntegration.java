/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.polaris.core.storage.hadoop;

import jakarta.annotation.Nonnull;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageInternalProperties;

import java.util.Set;

/** Placeholder for Hadoop credential handling */
public class HadoopCredentialsStorageIntegration
    extends InMemoryStorageIntegration<HadoopStorageConfigurationInfo> {

  public HadoopCredentialsStorageIntegration(
          HadoopStorageConfigurationInfo config
  ) {
    super(config, HadoopCredentialsStorageIntegration.class.getName());
  }

  @Override
  public AccessConfig getSubscopedCreds(@Nonnull RealmConfig realmConfig, boolean allowListOperation, @Nonnull Set<String> allowedReadLocations, @Nonnull Set<String> allowedWriteLocations) {
    AccessConfig.Builder accessConfig = AccessConfig.builder();

    // Set storage type for DefaultFileIOFactory
    accessConfig.putInternalProperty(StorageInternalProperties.STORAGE_TYPE_KEY, PolarisStorageConfigurationInfo.StorageType.HDFS.name());

    // Add Hadoop configuration resources as internal property
    String resources = config().getResources();
    if (resources != null && !resources.trim().isEmpty()) {
      accessConfig.putInternalProperty(StorageInternalProperties.HDFS_CONFIG_RESOURCES_KEY, resources);
    }

    // Add HDFS username as internal property if specified
    String username = config().getUsername();
    if (username != null && !username.trim().isEmpty()) {
      accessConfig.putInternalProperty(StorageInternalProperties.HDFS_USERNAME_KEY, username);
    }
    return accessConfig.build();
  }
}
