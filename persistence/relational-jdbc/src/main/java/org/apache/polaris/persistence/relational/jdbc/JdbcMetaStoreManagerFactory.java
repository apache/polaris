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

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
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
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
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

  final Map<String, PolarisMetaStoreManager> metaStoreManagerMap = new HashMap<>();
  final Map<String, EntityCache> entityCacheMap = new HashMap<>();
  final Map<String, Supplier<BasePersistence>> sessionSupplierMap = new HashMap<>();
  protected final PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();

  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;
  @Inject Instance<DataSource> dataSource;
  @Inject RelationalJdbcConfiguration relationalJdbcConfiguration;

  protected JdbcMetaStoreManagerFactory() {}

  protected PrincipalSecretsGenerator secretsGenerator(
      String realmId, @Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      return PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  protected PolarisMetaStoreManager createNewMetaStoreManager() {
    return new AtomicOperationMetaStoreManager();
  }

  private void initializeForRealm(
      DatasourceOperations datasourceOperations,
      RealmContext realmContext,
      RootCredentialsSet rootCredentialsSet) {
    // Materialize realmId so that background tasks that don't have an active
    // RealmContext (request-scoped bean) can still create a JdbcBasePersistenceImpl
    String realmId = realmContext.getRealmIdentifier();
    sessionSupplierMap.put(
        realmId,
        () ->
            new JdbcBasePersistenceImpl(
                datasourceOperations,
                secretsGenerator(realmId, rootCredentialsSet),
                storageIntegrationProvider,
                realmId));

    PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
    metaStoreManagerMap.put(realmId, metaStoreManager);
  }

  public DatasourceOperations getDatasourceOperations() {
    DatasourceOperations databaseOperations;
    try {
      databaseOperations = new DatasourceOperations(dataSource.get(), relationalJdbcConfiguration);
    } catch (SQLException sqlException) {
      throw new RuntimeException(sqlException);
    }
    return databaseOperations;
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
      if (!metaStoreManagerMap.containsKey(realm)) {
        DatasourceOperations datasourceOperations = getDatasourceOperations();
        try {
          // Run the set-up script to create the tables.
          datasourceOperations.executeScript(
              datasourceOperations
                  .getDatabaseType()
                  .openInitScriptResource(bootstrapOptions.schemaOptions()));
        } catch (SQLException e) {
          throw new RuntimeException(
              String.format("Error executing sql script: %s", e.getMessage()), e);
        }
        initializeForRealm(
            datasourceOperations, realmContext, bootstrapOptions.rootCredentialsSet());
        PrincipalSecretsResult secretsResult =
            bootstrapServiceAndCreatePolarisPrincipalForRealm(
                realmContext, metaStoreManagerMap.get(realm));
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
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(realmContext);
      BasePersistence session = getOrCreateSessionSupplier(realmContext).get();

      PolarisCallContext callContext = new PolarisCallContext(realmContext, session, diagServices);
      BaseResult result = metaStoreManager.purge(callContext);
      results.put(realm, result);

      sessionSupplierMap.remove(realm);
      metaStoreManagerMap.remove(realm);
    }

    return Map.copyOf(results);
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
      DatasourceOperations datasourceOperations = getDatasourceOperations();
      initializeForRealm(datasourceOperations, realmContext, null);
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    }
    return metaStoreManagerMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public synchronized Supplier<BasePersistence> getOrCreateSessionSupplier(
      RealmContext realmContext) {
    if (!sessionSupplierMap.containsKey(realmContext.getRealmIdentifier())) {
      DatasourceOperations datasourceOperations = getDatasourceOperations();
      initializeForRealm(datasourceOperations, realmContext, null);
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    } else {
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    }
    return sessionSupplierMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public synchronized EntityCache getOrCreateEntityCache(
      RealmContext realmContext, RealmConfig realmConfig) {
    if (!entityCacheMap.containsKey(realmContext.getRealmIdentifier())) {
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(realmContext);
      entityCacheMap.put(
          realmContext.getRealmIdentifier(),
          new InMemoryEntityCache(realmConfig, metaStoreManager));
    }

    return entityCacheMap.get(realmContext.getRealmIdentifier());
  }

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal.
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      RealmContext realmContext, PolarisMetaStoreManager metaStoreManager) {
    // While bootstrapping we need to act as a fake privileged context since the real
    // CallContext may not have been resolved yet.
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            realmContext,
            sessionSupplierMap.get(realmContext.getRealmIdentifier()).get(),
            diagServices);
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(polarisContext);
    }

    EntityResult preliminaryRootPrincipalLookup =
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

    EntityResult rootPrincipalLookup =
        metaStoreManager.readEntityByName(
            polarisContext,
            null,
            PolarisEntityType.PRINCIPAL,
            PolarisEntitySubType.NULL_SUBTYPE,
            PolarisEntityConstants.getRootPrincipalName());
    PrincipalSecretsResult secrets =
        metaStoreManager.loadPrincipalSecrets(
            polarisContext,
            PolarisEntity.of(rootPrincipalLookup.getEntity())
                .getInternalPropertiesAsMap()
                .get(PolarisEntityConstants.getClientIdPropertyName()));
    return secrets;
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
            realmContext,
            sessionSupplierMap.get(realmContext.getRealmIdentifier()).get(),
            diagServices);
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(polarisContext);
    }

    EntityResult rootPrincipalLookup =
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
