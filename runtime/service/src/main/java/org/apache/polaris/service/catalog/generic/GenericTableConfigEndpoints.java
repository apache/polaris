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
package org.apache.polaris.service.catalog.generic;

import com.google.common.collect.ImmutableSet;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.rest.PolarisEndpoints;
import org.apache.polaris.service.catalog.config.CatalogConfigEndpointContributor;

@ApplicationScoped
public class GenericTableConfigEndpoints {
  @Produces
  public CatalogConfigEndpointContributor genericTableEndpoints() {
    return GenericTableConfigEndpoints::getSupportedGenericTableEndpoints;
  }

  /**
   * Get the generic table endpoints. Returns GENERIC_TABLE_ENDPOINTS if ENABLE_GENERIC_TABLES is
   * set to true, otherwise, returns an empty set.
   */
  public static Set<Endpoint> getSupportedGenericTableEndpoints(RealmConfig realmConfig) {
    boolean genericTableEnabled = realmConfig.getConfig(FeatureConfiguration.ENABLE_GENERIC_TABLES);
    return genericTableEnabled ? PolarisEndpoints.GENERIC_TABLE_ENDPOINTS : ImmutableSet.of();
  }
}
