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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LocalPolarisMetaStoreManagerFactory.class);

  private boolean bootstrap;

  protected abstract StoreType createBackingStore(@Nonnull PolarisDiagnostics diagnostics);

  protected abstract PolarisMetaStoreSession createMetaStoreSession(
      @Nonnull StoreType store, @Nonnull RealmContext realmContext);

  protected PrincipalSecretsGenerator secretsGenerator(RealmContext realmContext) {
    if (bootstrap) {
      return PrincipalSecretsGenerator.bootstrap(realmContext.getRealmIdentifier());
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

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
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(List<String> realms) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();

    bootstrap = true;
    try {
      for (String realm : realms) {
        RealmContext realmContext = () -> realm;
        if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
          initializeForRealm(realmContext);
          PrincipalSecretsResult secretsResult =
              bootstrapServiceAndCreatePolarisPrincipalForRealm(
                  realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
          results.put(realmContext.getRealmIdentifier(), secretsResult);
        }
      }
    } finally {
      bootstrap = false;
    }

    return results;
  }

  @Override
  public void purgeRealms(List<String> realms) {
    for (String realm : realms) {
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(() -> realm);
      PolarisMetaStoreSession session = getOrCreateSessionSupplier(() -> realm).get();

      PolarisCallContext callContext = new PolarisCallContext(session, diagServices);
      metaStoreManager.purge(callContext);

      storageCredentialCacheMap.remove(realm);
      backingStoreMap.remove(realm);
      sessionSupplierMap.remove(realm);
      metaStoreManagerMap.remove(realm);
    }
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

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal. After that we rotate the root principal
   * credentials and print them to stdout
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      RealmContext realmContext, PolarisMetaStoreManager metaStoreManager) {
    // While bootstrapping we need to act as a fake privileged context since the real
    // CallContext hasn't even been resolved yet.
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            sessionSupplierMap.get(realmContext.getRealmIdentifier()).get(), diagServices);
    CallContext.setCurrentContext(CallContext.of(realmContext, polarisContext));

    PolarisMetaStoreManager.EntityResult preliminaryRootPrincipalLookup =
        metaStoreManager.readEntityByName(
            polarisContext,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootPrincipalName());
    if (preliminaryRootPrincipalLookup.isSuccess()) {
      String overrideMessage =
          "It appears this metastore manager has already been bootstrapped. "
              + "To continue bootstrapping, please first purge the metastore with the `purge` command.";
      LOGGER.error("\n\n {} \n\n", overrideMessage);
      throw new IllegalArgumentException(overrideMessage);
    }

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
    PrincipalSecretsResult rotatedSecrets =
        metaStoreManager.rotatePrincipalSecrets(
            polarisContext,
            secrets.getPrincipalClientId(),
            secrets.getPrincipalId(),
            false,
            secrets.getMainSecretHash());
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
