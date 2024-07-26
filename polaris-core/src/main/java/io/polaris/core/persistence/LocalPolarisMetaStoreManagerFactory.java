package io.polaris.core.persistence;

import io.micrometer.core.instrument.MeterRegistry;
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
public abstract class LocalPolarisMetaStoreManagerFactory<
        StoreType, SessionType extends PolarisMetaStoreSession>
    implements MetaStoreManagerFactory {

  Map<String, PolarisMetaStoreManager> metaStoreManagerMap = new HashMap<>();
  Map<String, StorageCredentialCache> storageCredentialCacheMap = new HashMap<>();
  Map<String, StoreType> backingStoreMap = new HashMap<>();
  Map<String, Supplier<PolarisMetaStoreSession>> sessionSupplierMap = new HashMap<>();
  protected PolarisDiagnostics diagServices = new PolarisDefaultDiagServiceImpl();

  protected PolarisStorageIntegrationProvider storageIntegration;

  private Logger logger =
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
  public void setMetricRegistry(MeterRegistry metricRegistry) {
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
   *
   * @param realmContext
   * @param metaStoreManager
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
   *
   * @param realmContext
   * @param metaStoreManager
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
      logger.error(
          "\n\n Realm {} is not bootstrapped, could not load root principal. Please run Bootstrap command. \n\n",
          realmContext.getRealmIdentifier());
      throw new IllegalStateException(
          "Realm is not bootstrapped, please run server in bootstrap mode.");
    }
  }
}
