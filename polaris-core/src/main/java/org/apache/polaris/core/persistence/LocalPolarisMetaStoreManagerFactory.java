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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisSecretsManager.PrincipalSecretsResult;
import org.apache.polaris.core.context.Realm;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.cache.EntityCache;
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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(LocalPolarisMetaStoreManagerFactory.class);

  private final Map<String, PolarisMetaStoreManager> metaStoreManagerMap = new HashMap<>();
  private final Map<String, StorageCredentialCache> storageCredentialCacheMap = new HashMap<>();
  private final Map<String, EntityCache> entityCacheMap = new HashMap<>();
  private final Map<String, Supplier<PolarisMetaStoreSession>> sessionSupplierMap = new HashMap<>();

  private final PolarisConfigurationStore configurationStore;
  private final PolarisDiagnostics diagnostics;
  private final Clock clock;

  protected LocalPolarisMetaStoreManagerFactory(
      PolarisConfigurationStore configurationStore, PolarisDiagnostics diagnostics, Clock clock) {
    this.configurationStore = configurationStore;
    this.diagnostics = diagnostics;
    this.clock = clock;
  }

  protected abstract StoreType createBackingStore(@Nonnull PolarisDiagnostics diagnostics);

  protected abstract PolarisMetaStoreSession createMetaStoreSession(
      @Nonnull StoreType store,
      @Nonnull Realm realm,
      @Nullable PolarisCredentialsBootstrap credentialsBootstrap,
      @Nonnull PolarisDiagnostics diagnostics);

  protected PrincipalSecretsGenerator secretsGenerator(
      Realm realm, @Nullable PolarisCredentialsBootstrap credentialsBootstrap) {
    if (credentialsBootstrap != null) {
      return PrincipalSecretsGenerator.bootstrap(realm.id(), credentialsBootstrap);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  private void initializeForRealm(Realm realm, PolarisCredentialsBootstrap credentialsBootstrap) {
    final StoreType backingStore = createBackingStore(diagnostics);
    sessionSupplierMap.put(
        realm.id(),
        () -> createMetaStoreSession(backingStore, realm, credentialsBootstrap, diagnostics));

    PolarisMetaStoreManager metaStoreManager =
        new PolarisMetaStoreManagerImpl(realm, diagnostics, configurationStore, clock);
    metaStoreManagerMap.put(realm.id(), metaStoreManager);
  }

  @Override
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(
      List<String> realms, PolarisCredentialsBootstrap credentialsBootstrap) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();

    for (String realmName : realms) {
      Realm realm = Realm.newRealm(realmName);
      if (!metaStoreManagerMap.containsKey(realm.id())) {
        initializeForRealm(realm, credentialsBootstrap);
        PrincipalSecretsResult secretsResult =
            bootstrapServiceAndCreatePolarisPrincipalForRealm(
                realm, metaStoreManagerMap.get(realm.id()));
        results.put(realm.id(), secretsResult);
      }
    }

    return results;
  }

  @Override
  public void purgeRealms(List<String> realms) {
    for (String realm : realms) {
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(Realm.newRealm(realm));
      PolarisMetaStoreSession session = getOrCreateSessionSupplier(Realm.newRealm(realm)).get();

      metaStoreManager.purge(session);

      storageCredentialCacheMap.remove(realm);
      sessionSupplierMap.remove(realm);
      metaStoreManagerMap.remove(realm);
    }
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(Realm realm) {
    if (!metaStoreManagerMap.containsKey(realm.id())) {
      initializeForRealm(realm, null);
      checkPolarisServiceBootstrappedForRealm(realm, metaStoreManagerMap.get(realm.id()));
    }
    return metaStoreManagerMap.get(realm.id());
  }

  @Override
  public synchronized Supplier<PolarisMetaStoreSession> getOrCreateSessionSupplier(Realm realm) {
    if (!sessionSupplierMap.containsKey(realm.id())) {
      initializeForRealm(realm, null);
      checkPolarisServiceBootstrappedForRealm(realm, metaStoreManagerMap.get(realm.id()));
    } else {
      checkPolarisServiceBootstrappedForRealm(realm, metaStoreManagerMap.get(realm.id()));
    }
    return sessionSupplierMap.get(realm.id());
  }

  @Override
  public synchronized StorageCredentialCache getOrCreateStorageCredentialCache(Realm realm) {
    if (!storageCredentialCacheMap.containsKey(realm.id())) {
      storageCredentialCacheMap.put(
          realm.id(), new StorageCredentialCache(diagnostics, configurationStore));
    }

    return storageCredentialCacheMap.get(realm.id());
  }

  @Override
  public synchronized EntityCache getOrCreateEntityCache(Realm realm) {
    if (!entityCacheMap.containsKey(realm.id())) {
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(realm);
      entityCacheMap.put(realm.id(), new EntityCache(metaStoreManager, diagnostics));
    }

    return entityCacheMap.get(realm.id());
  }

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal. After that we rotate the root principal
   * credentials and print them to stdout
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      Realm realm, PolarisMetaStoreManager metaStoreManager) {
    PolarisMetaStoreSession metaStoreSession = sessionSupplierMap.get(realm.id()).get();

    PolarisMetaStoreManager.EntityResult preliminaryRootPrincipalLookup =
        metaStoreManager.readEntityByName(
            metaStoreSession,
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

    metaStoreManager.bootstrapPolarisService(metaStoreSession);

    PolarisMetaStoreManager.EntityResult rootPrincipalLookup =
        metaStoreManager.readEntityByName(
            metaStoreSession,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootPrincipalName());
    PolarisPrincipalSecrets secrets =
        metaStoreManager
            .loadPrincipalSecrets(
                metaStoreSession,
                PolarisEntity.of(rootPrincipalLookup.getEntity())
                    .getInternalPropertiesAsMap()
                    .get(PolarisEntityConstants.getClientIdPropertyName()))
            .getPrincipalSecrets();
    PrincipalSecretsResult rotatedSecrets =
        metaStoreManager.rotatePrincipalSecrets(
            metaStoreSession,
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
      Realm realm, PolarisMetaStoreManager metaStoreManager) {

    PolarisMetaStoreSession metaStoreSession = sessionSupplierMap.get(realm.id()).get();

    PolarisMetaStoreManager.EntityResult rootPrincipalLookup =
        metaStoreManager.readEntityByName(
            metaStoreSession,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootPrincipalName());

    if (!rootPrincipalLookup.isSuccess()) {
      LOGGER.error(
          "\n\n Realm {} is not bootstrapped, could not load root principal. Please run Bootstrap command. \n\n",
          realm.id());
      throw new IllegalStateException(
          "Realm is not bootstrapped, please run server in bootstrap mode.");
    }
  }
}
