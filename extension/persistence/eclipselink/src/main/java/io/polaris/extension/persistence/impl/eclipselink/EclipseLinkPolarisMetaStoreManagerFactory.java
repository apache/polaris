package io.polaris.extension.persistence.impl.eclipselink;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.polaris.core.PolarisDiagnostics;
import io.polaris.core.context.RealmContext;
import io.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import io.polaris.core.persistence.PolarisMetaStoreManager;
import io.polaris.core.persistence.PolarisMetaStoreSession;
import org.jetbrains.annotations.NotNull;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using an EclipseLink based meta store to store and retrieve all Polaris metadata. It can be
 * configured through persistence.xml to use supported RDBMS as the meta store.
 */
@JsonTypeName("eclipse-link")
public class EclipseLinkPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<
        PolarisEclipseLinkStore, PolarisEclipseLinkMetaStoreSessionImpl> {
  @JsonProperty("conf-file")
  private String confFile;

  @JsonProperty("persistence-unit")
  private String persistenceUnitName;

  protected PolarisEclipseLinkStore createBackingStore(@NotNull PolarisDiagnostics diagnostics) {
    return new PolarisEclipseLinkStore(diagnostics);
  }

  protected PolarisMetaStoreSession createMetaStoreSession(
      @NotNull PolarisEclipseLinkStore store, @NotNull RealmContext realmContext) {
    return new PolarisEclipseLinkMetaStoreSessionImpl(
        store, storageIntegration, realmContext, confFile, persistenceUnitName);
  }
}
