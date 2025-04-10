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
package org.apache.polaris.extension.persistence.relational.jdbc;

import io.quarkus.arc.All;
import io.quarkus.arc.InjectableInstance;
import io.quarkus.arc.InstanceHandle;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.common.constraint.Nullable;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.sql.DataSource;

import jakarta.inject.Named;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.*;
import org.apache.polaris.core.persistence.*;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using a JDBC backed by SQL metastore.
 */
@ApplicationScoped
@Identifier("relational-jdbc")
public class JdbcMetaStoreManagerFactory implements MetaStoreManagerFactory {

  final Map<String, PolarisMetaStoreManager> metaStoreManagerMap = new HashMap<>();
  final Map<String, StorageCredentialCache> storageCredentialCacheMap = new HashMap<>();
  final Map<String, EntityCache> entityCacheMap = new HashMap<>();
  final Map<String, Supplier<BasePersistence>> sessionSupplierMap = new HashMap<>();
  protected final PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMetaStoreManagerFactory.class);

  protected JdbcMetaStoreManagerFactory() {
    this(null);
  }

//  @Inject
//  @Any
//  InjectableInstance<DataSource> allDataSources;
//
//  @Inject
//  @All
//  @SuppressWarnings("CdiInjectionPointsInspection")
//  List<InstanceHandle<DataSource>> dataSources;

  @Inject
  @io.quarkus.agroal.DataSource("postgresql1")
  DataSource dataSource;

//  public boolean doesDatasourceExist(String datasourceName) {
//    try {
//      // Attempt to inject a datasource with the given name
//      Instance<DataSource> namedDataSource = allDataSources.select(DataSource.class, new NamedLiteral(datasourceName));
//      return !namedDataSource.isUnsatisfied();
//    } catch (Exception e) {
//      // Handle any potential exceptions during lookup
//      return false;
//    }
//  }
//
//  // Helper class to create a @Named qualifier instance
//  private static class NamedLiteral extends jakarta.enterprise.util.AnnotationLiteral<Named> implements Named {
//    private final String value;
//
//    public NamedLiteral(String value) {
//      this.value = value;
//    }
//
//    @Override
//    public String value() {
//      return value;
//    }
//  }

  private final Map<String, DataSource> dataSourceMap = new HashMap<>();

  private final PolarisDiagnostics diagnostics;
  @Inject PolarisStorageIntegrationProvider storageIntegrationProvider;

  @Inject
  protected JdbcMetaStoreManagerFactory(@Nonnull PolarisDiagnostics diagnostics) {
    this.diagnostics = diagnostics;
  }

  /**
   * Subclasses can override this to inject different implementations of PolarisMetaStoreManager
   * into the existing realm-based setup flow.
   */
  protected PolarisMetaStoreManager createNewMetaStoreManager() {
    return new AtomicOperationMetaStoreManager();
  }

  private void initializeForRealm(
      RealmContext realmContext, RootCredentialsSet rootCredentialsSet) {
//    System.out.println("DS exists - 1 " + doesDatasourceExist("\"primary\""));
//    System.out.println("DS exists - 2 " + findDataSourceByName("\"primary\""));
//    System.out.println("DS exists - 3" + findDataSourceByName("postgresql"));
//    // find the relevant datasource
//    DataSource defaultDataSource = dataSourceMap.get(realmContext.getRealmIdentifier());
//    defaultDataSource = defaultDataSource != null ? defaultDataSource : dataSourceMap.get("unknown");

//    allDataSources.handles().iterator().forEachRemaining(dataSource -> {
//      System.out.println(" " + dataSource);
//    });
//    List<DataSource> l = allDataSources.stream().toList();
//    DataSource ds = allDataSources.select(DataSource.class).get();

    DatasourceOperations databaseOperations = new DatasourceOperations(dataSource);
    // TODO: see if we need to take script from Quarkus or can we just
    // use the script committed repo.
    // databaseOperations.executeScript();
    sessionSupplierMap.put(
        realmContext.getRealmIdentifier(),
        () ->
            new PolarisJdbcBasePersistenceImpl(
                realmContext.getRealmIdentifier(),
                databaseOperations,
                secretsGenerator(realmContext, rootCredentialsSet),
                storageIntegrationProvider));

    PolarisMetaStoreManager metaStoreManager = createNewMetaStoreManager();
    metaStoreManagerMap.put(realmContext.getRealmIdentifier(), metaStoreManager);
  }

  @Override
  public synchronized Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();

    for (String realm : realms) {
      RealmContext realmContext = () -> realm;
      if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
        initializeForRealm(realmContext, rootCredentialsSet);
        PrincipalSecretsResult secretsResult =
            bootstrapServiceAndCreatePolarisPrincipalForRealm(
                realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
        results.put(realmContext.getRealmIdentifier(), secretsResult);
      }
    }

    return Map.copyOf(results);
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> results = new HashMap<>();

    for (String realm : realms) {
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(() -> realm);
      BasePersistence session = getOrCreateSessionSupplier(() -> realm).get();

      PolarisCallContext callContext = new PolarisCallContext(session, diagServices);
      BaseResult result = metaStoreManager.purge(callContext);
      results.put(realm, result);

      storageCredentialCacheMap.remove(realm);
      sessionSupplierMap.remove(realm);
      metaStoreManagerMap.remove(realm);
    }

    return Map.copyOf(results);
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeForRealm(realmContext, null);
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    }
    return metaStoreManagerMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public synchronized Supplier<BasePersistence> getOrCreateSessionSupplier(
      RealmContext realmContext) {
    if (!sessionSupplierMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeForRealm(realmContext, null);
      checkPolarisServiceBootstrappedForRealm(
          realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    } else {
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
  public synchronized EntityCache getOrCreateEntityCache(RealmContext realmContext) {
    if (!entityCacheMap.containsKey(realmContext.getRealmIdentifier())) {
      PolarisMetaStoreManager metaStoreManager = getOrCreateMetaStoreManager(realmContext);
      entityCacheMap.put(realmContext.getRealmIdentifier(), new EntityCache(metaStoreManager));
    }

    return entityCacheMap.get(realmContext.getRealmIdentifier());
  }

  /**
   * This method bootstraps service for a given realm: i.e. creates all the needed entities in the
   * metastore and creates a root service principal. After that we rotate the root principal
   * credentials and print them to stdout
   */
  private PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      RealmContext realmContext, PolarisMetaStoreManager metaStoreManager) {
    // While bootstrapping we need to act as a fake privileged context since the real
    // CallContext may not have been resolved yet.
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            sessionSupplierMap.get(realmContext.getRealmIdentifier()).get(), diagServices);
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(CallContext.of(realmContext, polarisContext));
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
    if (CallContext.getCurrentContext() == null) {
      CallContext.setCurrentContext(CallContext.of(realmContext, polarisContext));
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

  protected PrincipalSecretsGenerator secretsGenerator(
      RealmContext realmContext, @Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      return PrincipalSecretsGenerator.bootstrap(
          realmContext.getRealmIdentifier(), rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

//  private DataSource findDataSourceByName(String dataSourceName) {
//    for (InstanceHandle<DataSource> handle : dataSources) {
//      String name = handle.getBean().getName();
//      System.out.println("Name of the datatosuce configure " + name + " " + handle.getBean().getIdentifier());
//      name = name == null ? "primary" : unquoteDataSourceName(name);
//      if (name.equals(dataSourceName)) {
//        return handle.get();
//      }
//    }
//    return null;
//  }
//  public static String unquoteDataSourceName(String dataSourceName) {
//    if (dataSourceName.startsWith("\"") && dataSourceName.endsWith("\"")) {
//      dataSourceName = dataSourceName.substring(1, dataSourceName.length() - 1);
//    }
//    return dataSourceName;
//  }
}
