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
import static org.apache.polaris.core.auth.AuthBootstrapUtil.createPolarisPrincipalForRealm;
import static org.apache.polaris.persistence.nosql.coretypes.refs.References.realmReferenceNames;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.ACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.CREATED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INACTIVE;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.INITIALIZING;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGED;
import static org.apache.polaris.persistence.nosql.realms.api.RealmDefinition.RealmStatus.PURGING;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.persistence.nosql.api.Persistence;
import org.apache.polaris.persistence.nosql.api.RealmPersistenceFactory;
import org.apache.polaris.persistence.nosql.authz.api.Privileges;
import org.apache.polaris.persistence.nosql.realms.api.RealmDefinition;
import org.apache.polaris.persistence.nosql.realms.api.RealmManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("nosql")
class NoSqlMetaStoreManagerFactory implements MetaStoreManagerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoSqlMetaStoreManagerFactory.class);

  private final Map<String, Persistence> realmPersistenceMap = new ConcurrentHashMap<>();
  private final RealmManagement realmManagement;
  private final RealmPersistenceFactory realmPersistenceFactory;
  private final Privileges privileges;
  private final PolarisStorageIntegrationProvider storageIntegrationProvider;
  private final Clock clock;
  private final PolarisDiagnostics diagnostics;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  NoSqlMetaStoreManagerFactory(
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
  }

  @PostConstruct
  void logRegisteredRealms() {
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

    return new NoSqlMetaStoreManager(() -> purgeRealm(realmId), null, clock);
  }

  private NoSqlMetaStore newPersistenceMetaStore(Persistence persistence) {
    return new NoSqlMetaStore(persistence, privileges, storageIntegrationProvider, diagnostics);
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
            case CREATED, LOADING, INITIALIZING, INACTIVE, PURGING ->
                // PURGING is the state telling the maintenance service to delete the realm data.
                PURGING;
            case ACTIVE -> INACTIVE;
            case PURGED ->
                // The PURGED state is also handled by the maintenance service.
                PURGED;
          };

      if (nextStatus == existing.status()) {
        // No status change (PURGING/PURGED), stop here.
        // Maintenance service will handle the actual deletion of the realm data.
        break;
      }

      var update = RealmDefinition.builder().from(existing).status(nextStatus).build();

      var updated = realmManagement.update(existing, update);

      if (updated.status() == PURGING || updated.status() == PURGED) {
        // Final status (PURGING/PURGED), stop here.
        // Maintenance service will handle the actual deletion of the realm data.
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
                      desc, RealmDefinition.builder().from(desc).status(INITIALIZING).build());
              return desc;
            });

    if (realmDesc.status() == CREATED) {
      realmDesc =
          realmManagement.update(
              realmDesc, RealmDefinition.builder().from(realmDesc).status(INITIALIZING).build());
    }

    checkState(
        realmDesc.status() == INITIALIZING,
        "Unexpected status '%s' for realm '%s'",
        realmDesc.status(),
        realmId);

    var persistence = buildRealmPersistence(realmId);
    persistence.createReferencesSilent(realmReferenceNames());
    var metaStore = newPersistenceMetaStore(persistence);
    var metaStoreManager =
        new NoSqlMetaStoreManager(
            () -> {
              throw new IllegalStateException("Cannot purge while bootstrapping");
            },
            rootCredentialsSet,
            clock);

    PolarisCallContext ctx = new PolarisCallContext(() -> realmId, metaStore);
    var secretsResult = createPolarisPrincipalForRealm(metaStoreManager, ctx);

    realmManagement.update(
        realmDesc, RealmDefinition.builder().from(realmDesc).status(ACTIVE).build());

    LOGGER.info("Realm '{}' has been successfully bootstrapped.", realmId);

    return secretsResult;
  }
}
