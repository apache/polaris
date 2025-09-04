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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.config.RealmConfigImpl;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.InMemoryEntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.persistence.transactional.TransactionalMetaStoreManagerImpl;
import org.apache.polaris.core.persistence.transactional.TransactionalPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The common implementation of Configuration interface for configuring the {@link
 * PolarisMetaStoreManager} using an underlying meta store to store and retrieve all Polaris
 * metadata.
 */
public abstract class LocalPolarisMetaStoreManagerFactory<StoreType>
    implements MetaStoreManagerFactory {

  final Map<String, EntityCache> entityCacheMap = new HashMap<>();
  final Map<String, StoreType> backingStoreMap = new HashMap<>();
  final Map<String, Supplier<TransactionalPersistence>> sessionSupplierMap = new HashMap<>();

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LocalPolarisMetaStoreManagerFactory.class);

  private final Clock clock;
  private final PolarisDiagnostics diagnostics;
  private final PolarisConfigurationStore configurationStore;

  protected LocalPolarisMetaStoreManagerFactory(
      @Nonnull Clock clock,
      @Nonnull PolarisDiagnostics diagnostics,
      @Nonnull PolarisConfigurationStore configurationStore) {
    this.clock = clock;
    this.diagnostics = diagnostics;
    this.configurationStore = configurationStore;
  }

  protected abstract StoreType createBackingStore(@Nonnull PolarisDiagnostics diagnostics);

  protected abstract TransactionalPersistence createMetaStoreSession(
      @Nonnull StoreType store,
      @Nonnull RealmContext realmContext,
      @Nullable RootCredentialsSet rootCredentialsSet,
      @Nonnull PolarisDiagnostics diagnostics);

  protected PrincipalSecretsGenerator secretsGenerator(
      RealmContext realmContext, @Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      return PrincipalSecretsGenerator.bootstrap(
          realmContext.getRealmIdentifier(), rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  /**
   * Subclasses can override this to inject different implementations of PolarisMetaStoreManager
   * into the existing realm-based setup flow.
   */
  protected PolarisMetaStoreManager createNewMetaStoreManager(
      Clock clock,
      PolarisDiagnostics diagnostics,
      RealmContext realmContext,
      RealmConfig realmConfig) {
    return new TransactionalMetaStoreManagerImpl(
        clock,
        diagnostics,
        realmContext,
        realmConfig,
        () -> createPersistenceSession(realmContext));
  }

  private void initializeForRealm(
      RealmContext realmContext, RootCredentialsSet rootCredentialsSet) {
    final StoreType backingStore = createBackingStore(diagnostics);
    sessionSupplierMap.put(
        realmContext.getRealmIdentifier(),
        () -> createMetaStoreSession(backingStore, realmContext, rootCredentialsSet, diagnostics));
  }

  @Override
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();

    for (String realm : realms) {
      RealmContext realmContext = () -> realm;
      if (!sessionSupplierMap.containsKey(realm)) {
        initializeForRealm(realmContext, rootCredentialsSet);
        PrincipalSecretsResult secretsResult =
            bootstrapServiceAndCreatePolarisPrincipalForRealm(realmContext);
        results.put(realm, secretsResult);
      }
    }

    return Map.copyOf(results);
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> results = new HashMap<>();

    for (String realm : realms) {
      RealmContext realmContext = () -> realm;
      PolarisMetaStoreManager metaStoreManager = createMetaStoreManager(realmContext);

      BaseResult result = metaStoreManager.purge();
      results.put(realm, result);

      backingStoreMap.remove(realm);
      sessionSupplierMap.remove(realm);
    }

    return Map.copyOf(results);
  }

  @Override
  public PolarisMetaStoreManager createMetaStoreManager(RealmContext realmContext) {
    RealmConfig realmConfig = new RealmConfigImpl(configurationStore, realmContext);
    return createNewMetaStoreManager(clock, diagnostics, realmContext, realmConfig);
  }

  protected synchronized TransactionalPersistence createPersistenceSession(
      RealmContext realmContext) {
    if (!sessionSupplierMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeForRealm(realmContext, null);
      checkPolarisServiceBootstrappedForRealm(realmContext);
    }
    return sessionSupplierMap.get(realmContext.getRealmIdentifier()).get();
  }

  @Override
  public synchronized EntityCache getOrCreateEntityCache(RealmContext realmContext) {
    return entityCacheMap.computeIfAbsent(
        realmContext.getRealmIdentifier(),
        realmId -> {
          RealmConfig realmConfig = new RealmConfigImpl(configurationStore, realmContext);
          return new InMemoryEntityCache(diagnostics, realmConfig);
        });
  }

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal.
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      RealmContext realmContext) {
    PolarisMetaStoreManager metaStoreManager = createMetaStoreManager(realmContext);

    Optional<PrincipalEntity> preliminaryRootPrincipal = metaStoreManager.findRootPrincipal();
    if (preliminaryRootPrincipal.isPresent()) {
      String overrideMessage =
          "It appears this metastore manager has already been bootstrapped. "
              + "To continue bootstrapping, please first purge the metastore with the `purge` command.";
      LOGGER.error("\n\n {} \n\n", overrideMessage);
      throw new IllegalArgumentException(overrideMessage);
    }

    metaStoreManager.bootstrapPolarisService();

    PrincipalEntity rootPrincipal = metaStoreManager.findRootPrincipal().orElseThrow();
    return metaStoreManager.loadPrincipalSecrets(rootPrincipal.getClientId());
  }

  /**
   * In this method we check if Service was bootstrapped for a given realm, i.e. that all the
   * entities were created (root principal, root principal role, etc) If service was not
   * bootstrapped we are throwing IllegalStateException exception That will cause service to crash
   * and force user to run Bootstrap command and initialize MetaStore and create all the required
   * entities
   */
  private void checkPolarisServiceBootstrappedForRealm(RealmContext realmContext) {
    PolarisMetaStoreManager metaStoreManager = createMetaStoreManager(realmContext);

    Optional<PrincipalEntity> rootPrincipal = metaStoreManager.findRootPrincipal();
    if (rootPrincipal.isEmpty()) {
      LOGGER.error(
          "\n\n Realm {} is not bootstrapped, could not load root principal. Please run Bootstrap command. \n\n",
          realmContext.getRealmIdentifier());
      throw new IllegalStateException(
          "Realm is not bootstrapped, please run server in bootstrap mode.");
    }
  }
}
