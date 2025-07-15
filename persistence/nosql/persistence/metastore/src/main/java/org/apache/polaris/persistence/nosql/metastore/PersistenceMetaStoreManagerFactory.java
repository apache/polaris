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
package org.apache.polaris.persistence.nosql.metastore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.polaris.persistence.nosql.metastore.TypeMapping.initializeRealmIfNecessary;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.polaris.async.AsyncExec;
import org.apache.polaris.async.Cancelable;
import org.apache.polaris.authz.api.Privileges;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.realms.api.RealmDefinition;
import org.apache.polaris.realms.api.RealmManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("nosql")
class PersistenceMetaStoreManagerFactory implements MetaStoreManagerFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PersistenceMetaStoreManagerFactory.class);

  private final Map<String, RealmState> realmStateMap = new ConcurrentHashMap<>();
  private final RealmManagement realmManagement;
  private final Cancelable<Void> scheduled;
  private final RealmPersistenceFactory realmPersistenceFactory;
  private final PolarisDiagnostics diagServices;
  private final Privileges privileges;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final PolarisConfigurationStore configurationStore;
  private final Clock clock;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  PersistenceMetaStoreManagerFactory(
      RealmManagement realmManagement,
      AsyncExec asyncExec,
      RealmPersistenceFactory realmPersistenceFactory,
      PolarisDiagnostics diagServices,
      Privileges privileges,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      PolarisConfigurationStore configurationStore,
      Clock clock) {
    this.realmManagement = realmManagement;
    this.scheduled =
        asyncExec.schedulePeriodic(this::purgeNoLongerNeededRealmStructures, Duration.ofMinutes(5));
    this.realmPersistenceFactory = realmPersistenceFactory;
    this.diagServices = diagServices;
    this.privileges = privileges;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.configurationStore = configurationStore;
    this.clock = clock;

    realmManagement
        .list()
        .forEach(realmDefinition -> LOGGER.info("Realm registered: {}", realmDefinition));
  }

  @PreDestroy
  void shutdown() {
    scheduled.cancel();
  }

  @Override
  public Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();

    for (String realmId : realms) {
      var existing = realmManagement.get(realmId);
      if (existing.isPresent() && !existing.get().needsBootstrap()) {
        LOGGER.debug("Realm '{}' is already fully bootstrapped.", realmId);
        continue;
      }

      var secretsResult = bootstrapRealm(realmId, rootCredentialsSet, existing);
      results.put(realmId, secretsResult);
    }

    return Map.copyOf(results);
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    var results = new HashMap<String, BaseResult>();

    for (var realm : realms) {
      results.put(realm, purgeRealm(realm));
    }

    return Map.copyOf(results);
  }

  @Override
  public EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig) {
    // no `EntityCache`
    return null;
  }

  @Override
  public Supplier<BasePersistence> getOrCreateSessionSupplier(RealmContext realmContext) {
    var persistence = realmPersistence(realmContext);
    return () -> newPersistenceMetaStore(persistence);
  }

  @Override
  public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
    var state = realmState(realmContext);
    return new PersistenceMetaStoreManager(
        state.persistence(), () -> purgeRealm(realmContext.getRealmIdentifier()), null, this);
  }

  PersistenceMetaStore newPersistenceMetaStore(Persistence persistence) {
    return new PersistenceMetaStore(persistence, privileges, storageIntegrationProvider);
  }

  Persistence realmPersistence(RealmContext realmContext) {
    return realmPersistence(realmContext.getRealmIdentifier());
  }

  Persistence realmPersistence(String realm) {
    return realmState(realm).persistence();
  }

  /** Remove {@link StorageCredentialCache}s for no longer existing realms. */
  void purgeNoLongerNeededRealmStructures() {
    var existingIds = realmManagement.list().map(RealmDefinition::id).collect(Collectors.toSet());
    var toRemove = realmStateMap.keySet().stream().filter(id -> !existingIds.contains(id)).toList();
    toRemove.forEach(this::removeRealmState);
  }

  void removeRealmState(String realm) {
    realmStateMap.remove(realm);
  }

  RealmState realmState(RealmContext realmContext) {
    return realmState(realmContext.getRealmIdentifier());
  }

  RealmState realmState(String realmId) {
    var realmState = realmStateMap.get(realmId);
    if (realmState != null) {
      return realmState;
    }
    // This synchronization is there to prevent the CHM from locking and causing "strange" side
    // effects. A naive "computeIfAbsent" with 'initializeIfNecessary' called from the mapping
    // function could cause the CHM to lock for quite a long while.
    synchronized (this) {
      realmState = realmStateMap.get(realmId);
      if (realmState != null) {
        return realmState;
      }

      LOGGER.info("Checking realm '{}' on first use", realmId);

      var realmDesc = realmManagement.get(realmId);
      checkArgument(realmDesc.isPresent(), "Realm '%s' does not exist", realmId);
      checkArgument(
          !realmDesc.get().needsBootstrap(),
          "Realm '%s' has not been fully bootstrapped. Re-run the bootstrap admin command.",
          realmId);

      var persistence = getRealmPersistence(realmId);
      realmState = new RealmState(persistence);
      var ms = newPersistenceMetaStore(persistence);
      ms.initializeCatalogsIfNecessary();
      realmStateMap.put(realmId, realmState);
      LOGGER.info("Done checking realm '{}'", realmId);
      return realmState;
    }
  }

  private Persistence getRealmPersistence(String realmId) {
    return realmPersistenceFactory.newBuilder().realmId(realmId).build();
  }

  BaseResult purgeRealm(String realmId) {
    while (true) {
      var existingOptional = realmManagement.get(realmId);
      if (existingOptional.isEmpty()) {
        removeRealmState(realmId);
        return new BaseResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND);
      }
      var existing = existingOptional.get();

      var nextStatus =
          switch (existing.status()) {
            case CREATED, LOADING, INITIALIZING, INACTIVE -> RealmDefinition.RealmStatus.PURGING;
            case ACTIVE -> RealmDefinition.RealmStatus.INACTIVE;
            case PURGING ->
                // TODO this state should really happen during maintenance!!
                RealmDefinition.RealmStatus.PURGED;
            case PURGED -> RealmDefinition.RealmStatus.PURGED;
          };

      var update = RealmDefinition.builder().from(existing).status(nextStatus).build();

      var updated = realmManagement.update(existing, update);

      if (updated.status() == RealmDefinition.RealmStatus.PURGED) {
        realmManagement.delete(updated);
        break;
      }
    }

    removeRealmState(realmId);

    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /**
   * Bootstrap the given realm using the given root credentials.
   *
   * <p>Repeated calls to this function for the same realm is safe, the outcome is idempotent.
   */
  private PrincipalSecretsResult bootstrapRealm(
      String realmId, RootCredentialsSet rootCredentialsSet, Optional<RealmDefinition> existing) {
    LOGGER.info("Bootstrapping realm '{}' ...", realmId);

    // TODO later, update bootstrap to use RealmLifecycleCallbacks and leverage the other
    //  intermediate realm-states to make this function not racy

    var realmDesc =
        existing.orElseGet(
            () -> {
              var desc = realmManagement.create(realmId);
              // Move realm from CREATED to INITIALIZING state
              desc =
                  realmManagement.update(
                      desc,
                      RealmDefinition.builder()
                          .from(desc)
                          .status(RealmDefinition.RealmStatus.INITIALIZING)
                          .build());
              return desc;
            });

    if (realmDesc.status() == RealmDefinition.RealmStatus.CREATED) {
      realmDesc =
          realmManagement.update(
              realmDesc,
              RealmDefinition.builder()
                  .from(realmDesc)
                  .status(RealmDefinition.RealmStatus.INITIALIZING)
                  .build());
    }

    checkState(
        realmDesc.status() == RealmDefinition.RealmStatus.INITIALIZING,
        "Unexpected status '%s' for realm '%s'",
        realmDesc.status(),
        realmId);

    var persistence = getRealmPersistence(realmId);
    initializeRealmIfNecessary(persistence);
    var metaStoreManager =
        new PersistenceMetaStoreManager(
            persistence,
            () -> {
              throw new IllegalStateException("Cannot purge while bootstrapping");
            },
            rootCredentialsSet,
            this);
    var metaStore = newPersistenceMetaStore(persistence);

    var secretsResult =
        bootstrapServiceAndCreatePolarisPrincipalForRealm(
            () -> realmId, metaStoreManager, metaStore, rootCredentialsSet);

    realmManagement.update(
        realmDesc,
        RealmDefinition.builder()
            .from(realmDesc)
            .status(RealmDefinition.RealmStatus.ACTIVE)
            .build());

    LOGGER.info("Realm '{}' has been successfully bootstrapped.", realmId);

    return secretsResult;
  }

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal. After that we rotate the root principal
   * credentials and print them to stdout
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      RealmContext realmContext,
      PersistenceMetaStoreManager metaStoreManager,
      PersistenceMetaStore metaStore,
      RootCredentialsSet rootCredentialsSet) {
    // While bootstrapping we need to act as a fake privileged context since the real
    // CallContext may not have been resolved yet.
    var polarisContext =
        new PolarisCallContext(realmContext, metaStore, diagServices, configurationStore, clock);

    // TODO nuke these damn ThreadLocals
    var prevCallContext = CallContext.getCurrentContext();
    try {
      CallContext.setCurrentContext(polarisContext);

      var createPrincipalResult = metaStoreManager.bootstrapPolarisServiceInternal(polarisContext);

      var rootPrincipal =
          createPrincipalResult
              .map(CreatePrincipalResult::getPrincipal)
              .orElseGet(
                  () ->
                      metaStoreManager
                          .readEntityByName(
                              polarisContext,
                              null,
                              PolarisEntityType.PRINCIPAL,
                              PolarisEntitySubType.NULL_SUBTYPE,
                              PolarisEntityConstants.getRootPrincipalName())
                          .getEntity());

      var secrets =
          metaStoreManager
              .loadPrincipalSecrets(
                  polarisContext,
                  PolarisEntity.of(rootPrincipal)
                      .getInternalPropertiesAsMap()
                      .get(PolarisEntityConstants.getClientIdPropertyName()))
              .getPrincipalSecrets();

      var principalSecrets = createPrincipalResult.map(CreatePrincipalResult::getPrincipalSecrets);
      if (principalSecrets.isPresent()) {
        LOGGER.debug(
            "Root principal created for realm '{}', directly returning credentials for client-ID '{}'",
            realmContext.getRealmIdentifier(),
            principalSecrets.get().getPrincipalClientId());
        return new PrincipalSecretsResult(principalSecrets.get());
      }

      var providedCredentials =
          rootCredentialsSet.credentials().get(realmContext.getRealmIdentifier());
      if (providedCredentials != null) {
        LOGGER.debug(
            "Root principal for realm '{}' already exists, credentials provided externally, returning credentials for client-ID '{}'",
            realmContext.getRealmIdentifier(),
            providedCredentials.clientId());
        return new PrincipalSecretsResult(
            new PolarisPrincipalSecrets(
                rootPrincipal.getId(),
                providedCredentials.clientId(),
                providedCredentials.clientSecret(),
                providedCredentials.clientSecret()));
      }

      // Have to rotate the secrets to retain the idempotency of this function
      var result =
          metaStoreManager.rotatePrincipalSecrets(
              polarisContext,
              secrets.getPrincipalClientId(),
              secrets.getPrincipalId(),
              false,
              secrets.getMainSecretHash());
      LOGGER.debug(
          "Rotating credentials for root principal for realm '{}', client-ID is '{}'",
          realmContext.getRealmIdentifier(),
          result.getPrincipalSecrets().getPrincipalClientId());
      return result;
    } finally {
      CallContext.setCurrentContext(prevCallContext);
    }
  }
}
