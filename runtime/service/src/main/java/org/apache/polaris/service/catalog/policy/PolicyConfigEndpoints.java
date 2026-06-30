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
package org.apache.polaris.service.catalog.policy;

import com.google.common.collect.ImmutableSet;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.rest.CatalogConfigEndpointContributor;

@ApplicationScoped
public class PolicyConfigEndpoints {
  @Produces
  public CatalogConfigEndpointContributor policyEndpoints(RealmConfig realmConfig) {
    return () -> getSupportedPolicyEndpoints(realmConfig);
  }

  /**
   * Get the policy store endpoints. Returns POLICY_ENDPOINTS if ENABLE_POLICY_STORE is set to true,
   * otherwise, returns an empty set.
   */
  public static Set<Endpoint> getSupportedPolicyEndpoints(RealmConfig realmConfig) {
    boolean policyStoreEnabled = realmConfig.getConfig(FeatureConfiguration.ENABLE_POLICY_STORE);
    return policyStoreEnabled ? PolicyEndpoints.POLICY_STORE_ENDPOINTS : ImmutableSet.of();
  }
}
