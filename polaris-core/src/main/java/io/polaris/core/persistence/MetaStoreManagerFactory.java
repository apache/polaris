package io.polaris.core.persistence;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import io.micrometer.core.instrument.MeterRegistry;
import io.polaris.core.context.RealmContext;
import io.polaris.core.storage.PolarisStorageIntegrationProvider;
import io.polaris.core.storage.cache.StorageCredentialCache;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Configuration interface for configuring the {@link PolarisMetaStoreManager} via Dropwizard
 * configuration
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
public interface MetaStoreManagerFactory extends Discoverable {

  PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext);

  Supplier<PolarisMetaStoreSession> getOrCreateSessionSupplier(RealmContext realmContext);

  StorageCredentialCache getOrCreateStorageCredentialCache(RealmContext realmContext);

  void setStorageIntegrationProvider(PolarisStorageIntegrationProvider storageIntegrationProvider);

  void setMetricRegistry(MeterRegistry metricRegistry);

  Map<String, PolarisMetaStoreManager.PrincipalSecretsResult> bootstrapRealms(List<String> realms);
}
