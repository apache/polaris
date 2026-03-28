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
package org.apache.polaris.persistence.relational.jdbc;

import static org.apache.polaris.core.auth.AuthBootstrapUtil.createPolarisPrincipalForRealm;

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.BehaviorChangeConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableBootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableSchemaOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.InMemoryEntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using a JDBC backed by SQL metastore. TODO: refactor - <a
 * href="https://github.com/apache/polaris/pull/1287/files#r2047487588">...</a>
 */
@ApplicationScoped
@Identifier("relational-jdbc")
public class JdbcMetaStoreManagerFactory implements MetaStoreManagerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetaStoreManagerFactory.class);

  // Stateful per-realm cache — InMemoryEntityCache accumulates entries across requests
  final Map<String, EntityCache> entityCacheMap = new ConcurrentHashMap<>();

  // Cached per-realm schema version — loaded from DB once, stable at runtime
  private final ConcurrentHashMap<String, Integer> schemaVersionCache = new ConcurrentHashMap<>();

  // Tracks realms that have already passed the bootstrap verification check
  // (checkPolarisServiceBootstrappedForRealm), avoiding redundant DB hits on subsequent calls.
  private final Set<String> verifiedRealms = ConcurrentHashMap.newKeySet();

  // Cached DatasourceOperations — created once, reused across all requests.
  private volatile DatasourceOperations cachedDatasourceOperations;

  // Per-realm ReadWriteLock: read lock for request-path methods, write lock for purge.
  // Fair mode ensures purge is not starved under heavy read traffic.
  // Requests for different realms never contend; purge of one realm only blocks that realm.
  private final ConcurrentHashMap<String, ReadWriteLock> realmLocks = new ConcurrentHashMap<>();

  @Inject Clock clock;
  @Inject PolarisDiagnostics diagnostics;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject Instance<DataSource> dataSource;
  @Inject RelationalJdbcConfiguration relationalJdbcConfiguration;
  @Inject RealmConfig realmConfig;

  protected JdbcMetaStoreManagerFactory() {}

  private ReadWriteLock realmLock(String realmId) {
    return realmLocks.computeIfAbsent(realmId, k -> new ReentrantReadWriteLock(true));
  }

  protected PrincipalSecretsGenerator secretsGenerator(
      String realmId, @Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      return PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  protected PolarisMetaStoreManager createNewMetaStoreManager() {
    return new AtomicOperationMetaStoreManager(clock, diagnostics);
  }

  /** Loads and caches the schema version for the given realm (DB hit only on first call). */
  private int getOrLoadSchemaVersion(DatasourceOperations datasourceOperations, String realmId) {
    return schemaVersionCache.computeIfAbsent(
        realmId,
        k ->
            JdbcBasePersistenceImpl.loadSchemaVersion(
                datasourceOperations,
                realmConfig.getConfig(
                    BehaviorChangeConfiguration.SCHEMA_VERSION_FALL_BACK_ON_DNE)));
  }

  /** Creates a new stateless {@link JdbcBasePersistenceImpl} for the given realm. */
  private BasePersistence createSession(
      DatasourceOperations datasourceOperations,
      String realmId,
      @Nullable RootCredentialsSet rootCredentialsSet) {
    int schemaVersion = getOrLoadSchemaVersion(datasourceOperations, realmId);
    return new JdbcBasePersistenceImpl(
        diagnostics,
        datasourceOperations,
        secretsGenerator(realmId, rootCredentialsSet),
        storageIntegrationProvider,
        realmId,
        schemaVersion);
  }

  public DatasourceOperations getDatasourceOperations() {
    DatasourceOperations ops = cachedDatasourceOperations;
    if (ops == null) {
      synchronized (this) {
        ops = cachedDatasourceOperations;
        if (ops == null) {
          try {
            ops = new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
          } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
          }
          cachedDatasourceOperations = ops;
        }
      }
    }
    return ops;
  }

  @Override
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    SchemaOptions schemaOptions = ImmutableSchemaOptions.builder().build();

    BootstrapOptions bootstrapOptions =
        ImmutableBootstrapOptions.builder()
            .realms(realms)
            .rootCredentialsSet(rootCredentialsSet)
            .schemaOptions(schemaOptions)
            .build();

    return bootstrapRealms(bootstrapOptions);
  }

  @Override
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(
      BootstrapOptions bootstrapOptions) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();

    for (String realm : bootstrapOptions.realms()) {
      RealmContext realmContext = () -> realm;
      if (!verifiedRealms.contains(realm)) {
        DatasourceOperations datasourceOperations = getDatasourceOperations();
        int currentSchemaVersion =
            JdbcBasePersistenceImpl.loadSchemaVersion(datasourceOperations, true);
        int requestedSchemaVersion =
            JdbcBootstrapUtils.getRequestedSchemaVersion(bootstrapOptions);
        int effectiveSchemaVersion =
            JdbcBootstrapUtils.getRealmBootstrapSchemaVersion(
                datasourceOperations.getDatabaseType(),
                currentSchemaVersion,
                requestedSchemaVersion,
                JdbcBasePersistenceImpl.entityTableExists(datasourceOperations));
        LOGGER.info(
            "Effective schema version: {} for bootstrapping realm: {}",
            effectiveSchemaVersion,
            realm);
        try {
          // Run the set-up script to create the tables.
          datasourceOperations.executeScript(
              datasourceOperations
                  .getDatabaseType()
                  .openInitScriptResource(effectiveSchemaVersion));
        } catch (SQLException e) {
          throw new RuntimeException(
              String.format("Error executing sql script: %s", e.getMessage()), e);
        }
        // Cache the effective schema version for this realm
        schemaVersionCache.put(realm, effectiveSchemaVersion);

        PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
        BasePersistence metaStore =
            createSession(datasourceOperations, realm, bootstrapOptions.rootCredentialsSet());
        PolarisCallContext polarisContext = new PolarisCallContext(realmContext, metaStore);

        PrincipalSecretsResult secretsResult =
            createPolarisPrincipalForRealm(metaStoreManager, polarisContext);
        results.put(realm, secretsResult);
        verifiedRealms.add(realm);
      }
    }

    return Map.copyOf(results);
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> results = new HashMap<>();

    for (String realm : realms) {
      realmLock(realm).writeLock().lock();
      try {
        RealmContext realmContext = () -> realm;
        PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
        DatasourceOperations datasourceOperations = getDatasourceOperations();
        BasePersistence session = createSession(datasourceOperations, realm, null);

        PolarisCallContext callContext = new PolarisCallContext(realmContext, session);
        BaseResult result = metaStoreManager.purge(callContext);
        results.put(realm, result);

        // Evict all cached state for this realm
        entityCacheMap.remove(realm);
        schemaVersionCache.remove(realm);
        verifiedRealms.remove(realm);
      } finally {
        realmLock(realm).writeLock().unlock();
      }
    }

    return Map.copyOf(results);
  }

  @Override
  public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
    // Stateless — create a fresh instance on every call, no caching needed
    return createNewMetaStoreManager();
  }

  @Override
  public BasePersistence getOrCreateSession(RealmContext realmContext) {
    String realmId = realmContext.getRealmIdentifier();
    realmLock(realmId).readLock().lock();
    try {
      DatasourceOperations datasourceOperations = getDatasourceOperations();

      // Verify bootstrap once per realm lifetime; skip on subsequent calls.
      // On cold start, multiple threads may verify concurrently — this is benign
      // (idempotent DB query), trading a few redundant queries for simpler code.
      if (!verifiedRealms.contains(realmId)) {
        checkPolarisServiceBootstrappedForRealm(realmContext, datasourceOperations);
      }

      // Stateless — create a fresh instance on every call; schemaVersion is cached per realm
      return createSession(datasourceOperations, realmId, null);
    } finally {
      realmLock(realmId).readLock().unlock();
    }
  }

  @Override
  public EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig) {
    String realmId = realmContext.getRealmIdentifier();
    realmLock(realmId).readLock().lock();
    try {
      // EntityCache is stateful (Caffeine + ConcurrentHashMap) — must be shared across requests
      return entityCacheMap.computeIfAbsent(
          realmId,
          k -> {
            PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
            return new InMemoryEntityCache(diagnostics, realmConfig, metaStoreManager);
          });
    } finally {
      realmLock(realmId).readLock().unlock();
    }
  }

  /**
   * In this method we check if Service was bootstrapped for a given realm, i.e. that all the
   * entities were created (root principal, root principal role, etc) If service was not bootstrapped
   * we are throwing IllegalStateException exception That will cause service to crash and force user
   * to run Bootstrap command and initialize MetaStore and create all the required entities
   */
  private void checkPolarisServiceBootstrappedForRealm(
      RealmContext realmContext, DatasourceOperations datasourceOperations) {
    String realmId = realmContext.getRealmIdentifier();
    PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
    BasePersistence metaStore = createSession(datasourceOperations, realmId, null);
    PolarisCallContext polarisContext = new PolarisCallContext(realmContext, metaStore);

    Optional<PrincipalEntity> rootPrincipal = metaStoreManager.findRootPrincipal(polarisContext);
    if (rootPrincipal.isEmpty()) {
      LOGGER.error(
          "\n\n Realm {} is not bootstrapped, could not load root principal. Please run Bootstrap command. \n\n",
          realmId);
      throw new IllegalStateException(
          "Realm is not bootstrapped, please run server in bootstrap mode.");
    }
    verifiedRealms.add(realmId);
  }
}
