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
package org.apache.polaris.service.catalog.directory;

import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.rest.DirectoryEndpoints;
import org.apache.polaris.service.catalog.spi.CatalogConfigEndpointContributor;

@ApplicationScoped
@Priority(350)
public class DirectoryConfigEndpoints implements CatalogConfigEndpointContributor {
  private final RealmConfig realmConfig;

  @Inject
  public DirectoryConfigEndpoints(RealmConfig realmConfig) {
    this.realmConfig = realmConfig;
  }

  @Override
  public Set<Endpoint> endpoints() {
    return getSupportedDirectoryEndpoints(realmConfig);
  }

  /**
   * Get the directory endpoints. Returns DIRECTORY_ENDPOINTS if ENABLE_DIRECTORIES is set to true,
   * plus the scan endpoint if ENABLE_DIRECTORY_SCAN is also true, otherwise, returns an empty set.
   */
  public static Set<Endpoint> getSupportedDirectoryEndpoints(RealmConfig realmConfig) {
    if (!realmConfig.getConfig(FeatureConfiguration.ENABLE_DIRECTORIES)) {
      return ImmutableSet.of();
    }
    if (!realmConfig.getConfig(FeatureConfiguration.ENABLE_DIRECTORY_SCAN)) {
      return DirectoryEndpoints.DIRECTORY_ENDPOINTS;
    }
    return ImmutableSet.<Endpoint>builder()
        .addAll(DirectoryEndpoints.DIRECTORY_ENDPOINTS)
        .add(DirectoryEndpoints.V1_SCAN_DIRECTORY)
        .build();
  }
}
