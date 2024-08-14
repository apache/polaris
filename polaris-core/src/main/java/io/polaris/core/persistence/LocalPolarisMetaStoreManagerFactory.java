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
package io.polaris.core.persistence;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisDefaultDiagServiceImpl;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.context.CallContext;
import io.polaris.core.context.RealmContext;
import io.polaris.core.entity.PolarisEntity;
import io.polaris.core.entity.PolarisEntityConstants;
import io.polaris.core.entity.PolarisEntitySubType;
import io.polaris.core.entity.PolarisEntityType;
import io.polaris.core.entity.PolarisPrincipalSecrets;
import io.polaris.core.monitor.PolarisMetricRegistry;
import io.polaris.core.storage.PolarisStorageIntegrationProvider;
import io.polaris.core.storage.cache.StorageCredentialCache;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

/**
 * The common implementation of Configuration interface for configuring the {@link
 * PolarisMetaStoreManager} using an underlying meta store to store and retrieve all Polaris
 * metadata.
 */
public abstract class LocalPolarisMetaStoreManagerFactory<StoreType>
    implements MetaStoreManagerFactory {

  final Map<String, PolarisMetaStoreManager> metaStoreManagerMap = new HashMap<>();
  final Map<String, StorageCredentialCache> storageCredentialCacheMap = new HashMap<>();
  final Map<String, StoreType> backingStoreMap = new HashMap<>();
  final Map<String, Supplier<PolarisMetaStoreSession>> sessionSupplierMap = new HashMap<>();
  protected final PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();

  protected PolarisStorageIntegrationProvider storageIntegration;

  private final Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(LocalPolarisMetaStoreManagerFactory.class);

  protected abstract StoreType createBackingStore(@NotNull PolarisDiagnostics diagnostics);

  protected abstract PolarisMetaStoreSession createMetaStoreSession(
      @NotNull StoreType store, @NotNull RealmContext realmContext);

  private void initializeForRealm(RealmContext realmContext) {
    final StoreType backingStore = createBackingStore(diagServices);
    backingStoreMap.put(realmContext.getRealmIdentifier(), backingStore);
    sessionSupplierMap.put(
        realmContext.getRealmIdentifier(),
        () -> createMetaStoreSession(backingStore, realmContext));

    PolarisMetaStoreManager metaStoreManager = new PolarisMetaStoreManagerImpl();
    metaStoreManagerMap.put(realmContext.getRealmIdentifier(), metaStoreManager);
  }

  @Override
  public synchronized Map<String, PolarisMetaStoreManager.PrincipalSecretsResult> bootstrapRealms(
      List<String> realms) {
    Map<String, PolarisMetaStoreManager.PrincipalSecretsResult> results = new HashMap<>();

    for (String realm : realms) {
      RealmContext realmContext = () -> realm;
      if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
        initializeForRealm(realmContext);
        PolarisMetaStoreManager.PrincipalSecretsResult secretsResult =
            bootstrapServiceAndCreatePolarisPrincipalForRealm(
                realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
        results.put(realmContext.getRealmIdentifier(), secretsResult);
      }
    }

    return results;
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeForRealm(realmContext);
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    }
    return metaStoreManagerMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public synchronized Supplier<PolarisMetaStoreSession> getOrCreateSessionSupplier(
      RealmContext realmContext) {
    if (!sessionSupplierMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeForRealm(realmContext);
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    }
    return sessionSupplierMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public synchronized StorageCredentialCache getOrCreateStorageCredentialCache(
      RealmContext realmContext) {
    if (!storageCredentialCacheMap.containsKey(realmContext.getRealmIdentifier())) {
      storageCredentialCacheMap.put(
          realmContext.getRealmIdentifier(), new StorageCredentialCache());
    }

    return storageCredentialCacheMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public void setMetricRegistry(PolarisMetricRegistry metricRegistry) {
    // no-op
  }

  @Override
  public void setStorageIntegrationProvider(PolarisStorageIntegrationProvider storageIntegration) {
    this.storageIntegration = storageIntegration;
  }

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal. After that we rotate the root principal
   * credentials and print them to stdout
   */
  private PolarisMetaStoreManager.PrincipalSecretsResult
      bootstrapServiceAndCreatePolarisPrincipalForRealm(
          RealmContext realmContext, PolarisMetaStoreManager metaStoreManager) {
    // While bootstrapping we need to act as a fake privileged context since the real
    // CallContext hasn't even been resolved yet.
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            sessionSupplierMap.get(realmContext.getRealmIdentifier()).get(), diagServices);
    CallContext.setCurrentContext(CallContext.of(realmContext, polarisContext));

    metaStoreManager.bootstrapPolarisService(polarisContext);

    PolarisMetaStoreManager.EntityResult rootPrincipalLookup =
        metaStoreManager.readEntityByName(
            polarisContext,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootPrincipalName());
    PolarisPrincipalSecrets secrets =
        metaStoreManager
            .loadPrincipalSecrets(
                polarisContext,
                PolarisEntity.of(rootPrincipalLookup.getEntity())
                    .getInternalPropertiesAsMap()
                    .get(PolarisEntityConstants.getClientIdPropertyName()))
            .getPrincipalSecrets();
    PolarisMetaStoreManager.PrincipalSecretsResult rotatedSecrets =
        metaStoreManager.rotatePrincipalSecrets(
            polarisContext,
            secrets.getPrincipalClientId(),
            secrets.getPrincipalId(),
            secrets.getMainSecret(),
            false);
    return rotatedSecrets;
  }

  /**
   * In this method we check if Service was bootstrapped for a given realm, i.e. that all the
   * entities were created (root principal, root principal role, etc) If service was not
   * bootstrapped we are throwing IllegalStateException exception That will cause service to crash
   * and force user to run Bootstrap command and initialize MetaStore and create all the required
   * entities
   */
  private void checkPolarisServiceBootstrappedForRealm(
      RealmContext realmContext, PolarisMetaStoreManager metaStoreManager) {
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            sessionSupplierMap.get(realmContext.getRealmIdentifier()).get(), diagServices);
    CallContext.setCurrentContext(CallContext.of(realmContext, polarisContext));

    PolarisMetaStoreManager.EntityResult rootPrincipalLookup =
        metaStoreManager.readEntityByName(
            polarisContext,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootPrincipalName());

    if (!rootPrincipalLookup.isSuccess()) {
      LOGGER.error(
          "\n\n Realm {} is not bootstrapped, could not load root principal. Please run Bootstrap command. \n\n",
          realmContext.getRealmIdentifier());
      throw new IllegalStateException(
          "Realm is not bootstrapped, please run server in bootstrap mode.");
    }
  }
}
