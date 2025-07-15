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
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.realmReferenceNames;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.authz.api.Privileges;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
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

  private final Map<String, Persistence> realmPersistenceMap = new ConcurrentHashMap<>();
  private final RealmManagement realmManagement;
  private final RealmPersistenceFactory realmPersistenceFactory;
  private final Privileges privileges;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final Clock clock;
  private final PolarisDiagnostics diagnostics;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  PersistenceMetaStoreManagerFactory(
      RealmManagement realmManagement,
      RealmPersistenceFactory realmPersistenceFactory,
      Privileges privileges,
      PolarisStorageIntegrationProvider storageIntegrationProvider,
      Clock clock,
      PolarisDiagnostics diagnostics) {
    this.realmManagement = realmManagement;
    this.realmPersistenceFactory = realmPersistenceFactory;
    this.privileges = privileges;
    this.storageIntegrationProvider = storageIntegrationProvider;
    this.clock = clock;
    this.diagnostics = diagnostics;

    try (var realms = realmManagement.list()) {
      realms.forEach(realmDefinition -> LOGGER.info("Realm registered: {}", realmDefinition));
    }
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
  public BasePersistence getOrCreateSession(RealmContext realmContext) {
    return newPersistenceMetaStore(initializedRealmPersistence(realmContext.getRealmIdentifier()));
  }

  @Override
  public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
    var realmId = realmContext.getRealmIdentifier();
    var persistence = initializedRealmPersistence(realmId);

    return new PersistenceMetaStoreManager(
        () -> purgeRealm(realmId), null, () -> newPersistenceMetaStore(persistence), clock);
  }

  private PersistenceMetaStore newPersistenceMetaStore(Persistence persistence) {
    return new PersistenceMetaStore(
        persistence, privileges, storageIntegrationProvider, diagnostics);
  }

  private Persistence initializedRealmPersistence(String realmId) {
    var persistence = realmPersistenceMap.get(realmId);
    if (persistence != null) {
      return persistence;
    }
    // This synchronization is there to prevent the CHM from locking and causing "strange" side
    // effects. A naive "computeIfAbsent" with 'initializeIfNecessary' called from the mapping
    // function could cause the CHM to lock for quite a long while.
    synchronized (this) {
      persistence = realmPersistenceMap.get(realmId);
      if (persistence != null) {
        return persistence;
      }

      LOGGER.info("Checking realm '{}' on first use", realmId);

      var realmDesc = realmManagement.get(realmId);
      checkArgument(realmDesc.isPresent(), "Realm '%s' does not exist", realmId);
      checkArgument(
          !realmDesc.get().needsBootstrap(),
          "Realm '%s' has not been fully bootstrapped. Re-run the bootstrap admin command.",
          realmId);

      persistence = buildRealmPersistence(realmId);
      var ms = newPersistenceMetaStore(persistence);
      ms.initializeCatalogsIfNecessary();
      realmPersistenceMap.put(realmId, persistence);
      LOGGER.info("Done checking realm '{}'", realmId);
      return persistence;
    }
  }

  private Persistence buildRealmPersistence(String realmId) {
    return realmPersistenceFactory.newBuilder().realmId(realmId).build();
  }

  BaseResult purgeRealm(String realmId) {
    while (true) {
      var existingOptional = realmManagement.get(realmId);
      if (existingOptional.isEmpty()) {
        realmPersistenceMap.remove(realmId);
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

    realmPersistenceMap.remove(realmId);

    return new BaseResult(BaseResult.ReturnStatus.SUCCESS);
  }

  /**
   * Bootstrap the given realm using the given root credentials.
   *
   * <p>Repeated calls to this function for the same realm are safe, the outcome is idempotent.
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

    var persistence = buildRealmPersistence(realmId);
    persistence.createReferencesSilent(realmReferenceNames());
    var metaStore = newPersistenceMetaStore(persistence);
    var metaStoreManager =
        new PersistenceMetaStoreManager(
            () -> {
              throw new IllegalStateException("Cannot purge while bootstrapping");
            },
            rootCredentialsSet,
            () -> metaStore,
            clock);

    var secretsResult =
        bootstrapServiceAndCreatePolarisPrincipalForRealm(
            realmId, metaStoreManager, metaStore, rootCredentialsSet);

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
   * This method bootstraps service for a given realm: i.e., creates all the required entities in
   * the metastore and creates a root service principal. After that, we rotate the root principal
   * credentials and print them to stdout
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      String realmId,
      PersistenceMetaStoreManager metaStoreManager,
      PersistenceMetaStore metaStore,
      RootCredentialsSet rootCredentialsSet) {
    var createPrincipalResult = metaStoreManager.bootstrapPolarisServiceInternal();

    var rootPrincipal =
        createPrincipalResult
            .map(result -> (PolarisBaseEntity) result.getPrincipal())
            .orElseGet(
                () ->
                    metaStoreManager
                        .readEntityByName(
                            null,
                            PolarisEntityType.PRINCIPAL,
                            PolarisEntityConstants.getRootPrincipalName())
                        .getEntity());

    var clientId =
        PolarisEntity.of(rootPrincipal)
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getClientIdPropertyName());
    checkState(clientId != null, "Root principal has no client-ID");
    var secrets = metaStore.loadPrincipalSecrets(clientId);

    var principalSecrets = createPrincipalResult.map(CreatePrincipalResult::getPrincipalSecrets);
    if (principalSecrets.isPresent()) {
      LOGGER.debug(
          "Root principal created for realm '{}', directly returning credentials for client-ID '{}'",
          realmId,
          principalSecrets.get().getPrincipalClientId());
      return new PrincipalSecretsResult(principalSecrets.get());
    }

    var providedCredentials = rootCredentialsSet.credentials().get(realmId);
    if (providedCredentials != null) {
      LOGGER.debug(
          "Root principal for realm '{}' already exists, credentials provided externally, returning credentials for client-ID '{}'",
          realmId,
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
            secrets.getPrincipalId(), false, secrets.getMainSecretHash());
    LOGGER.debug(
        "Rotating credentials for root principal for realm '{}', client-ID is '{}'",
        realmId,
        result.getPrincipalSecrets().getPrincipalClientId());
    return result;
  }
}
