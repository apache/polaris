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
package org.apache.polaris.core.persistence;

import java.util.Map;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.metrics.MetricsPersistence;
import org.apache.polaris.core.policy.PolicyMappingPersistence;

/** Configuration interface for configuring the {@link PolarisMetaStoreManager}. */
public interface MetaStoreManagerFactory {

  PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext);

  /**
   * Returns the per-realm {@link BasePersistence}. Backends implement this on the same concrete
   * class as the other modular persistences (such as {@link PolicyMappingPersistence}, {@link
   * MetricsPersistence}, and {@link IntegrationPersistence}); the corresponding typed accessors
   * below default to casting that same instance.
   */
  BasePersistence getOrCreateBasePersistence(RealmContext realmContext);

  /**
   * Returns the per-realm {@link PolicyMappingPersistence}. The default implementation returns the
   * same instance produced by {@link #getOrCreateBasePersistence(RealmContext)} when it also
   * implements {@link PolicyMappingPersistence}.
   */
  default PolicyMappingPersistence getOrCreatePolicyMappingPersistence(RealmContext realmContext) {
    return cast(getOrCreateBasePersistence(realmContext), PolicyMappingPersistence.class);
  }

  /**
   * Returns the per-realm {@link MetricsPersistence}. The default implementation returns the same
   * instance produced by {@link #getOrCreateBasePersistence(RealmContext)} when it also implements
   * {@link MetricsPersistence}.
   */
  default MetricsPersistence getOrCreateMetricsPersistence(RealmContext realmContext) {
    return cast(getOrCreateBasePersistence(realmContext), MetricsPersistence.class);
  }

  /**
   * Returns the per-realm {@link IntegrationPersistence}. The default implementation returns the
   * same instance produced by {@link #getOrCreateBasePersistence(RealmContext)} when it also
   * implements {@link IntegrationPersistence}.
   */
  default IntegrationPersistence getOrCreateIntegrationPersistence(RealmContext realmContext) {
    return cast(getOrCreateBasePersistence(realmContext), IntegrationPersistence.class);
  }

  private static <T> T cast(BasePersistence base, Class<T> type) {
    if (type.isInstance(base)) {
      return type.cast(base);
    }
    throw new UnsupportedOperationException(
        "Persistence backend "
            + base.getClass().getName()
            + " does not implement "
            + type.getName());
  }

  EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig);

  Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet);

  default Map<String, PrincipalSecretsResult> bootstrapRealms(BootstrapOptions bootstrapOptions) {
    return bootstrapRealms(bootstrapOptions.realms(), bootstrapOptions.rootCredentialsSet());
  }

  /** Purge all metadata for the realms provided */
  Map<String, BaseResult> purgeRealms(Iterable<String> realms);
}
