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

import static org.apache.polaris.core.persistence.PrincipalSecretsGenerator.RANDOM_SECRETS;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.apache.polaris.core.persistence.transactional.TreeMapMetaStore;
import org.apache.polaris.core.persistence.transactional.TreeMapTransactionalPersistenceImpl;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class LocalPolarisMetaStoreManagerFactoryTest {

  private static final PolarisDiagnostics DIAG = new PolarisDefaultDiagServiceImpl();

  static class TestFactory extends LocalPolarisMetaStoreManagerFactory<TreeMapMetaStore> {

    TestFactory() {
      super(Clock.systemDefaultZone(), DIAG);
    }

    @Override
    protected TreeMapMetaStore createBackingStore(@NonNull PolarisDiagnostics diagnostics) {
      return new TreeMapMetaStore(diagnostics);
    }

    @Override
    protected TransactionalPersistence createMetaStoreSession(
        @NonNull TreeMapMetaStore store,
        @NonNull RealmContext realmContext,
        @Nullable RootCredentialsSet rootCredentialsSet,
        @NonNull PolarisDiagnostics diagnostics) {
      return new TreeMapTransactionalPersistenceImpl(
          diagnostics, store, Mockito.mock(), RANDOM_SECRETS);
    }
  }

  @Test
  void purgeRealmsClearsEntityCache() {
    TestFactory factory = new TestFactory();
    String realm = "test-realm";
    RootCredentialsSet creds =
        RootCredentialsSet.fromList(List.of(realm + ",clientId,clientSecret"));

    factory.bootstrapRealms(List.of(realm), creds);

    RealmContext realmContext = () -> realm;
    RealmConfig realmConfig = Mockito.mock(RealmConfig.class);
    Mockito.when(realmConfig.getConfig(FeatureConfiguration.ENTITY_CACHE_WEIGHER_TARGET))
        .thenReturn(100_000L);
    Mockito.when(realmConfig.getConfig(BehaviorChangeConfiguration.ENTITY_CACHE_SOFT_VALUES))
        .thenReturn(false);

    // Get the entity cache — this populates entityCacheMap
    EntityCache cacheBeforePurge = factory.getOrCreateEntityCache(realmContext, realmConfig);
    assertThat(cacheBeforePurge).isNotNull();

    // Purge the realm
    factory.purgeRealms(List.of(realm));

    // Re-bootstrap the realm
    factory.bootstrapRealms(List.of(realm), creds);

    // Get entity cache again — should be a fresh instance, not the stale one
    EntityCache cacheAfterPurge = factory.getOrCreateEntityCache(realmContext, realmConfig);
    assertThat(cacheAfterPurge).isNotNull();
    assertThat(cacheAfterPurge).isNotSameAs(cacheBeforePurge);
  }
}
