package io.polaris.core;

import io.polaris.core.persistence.PolarisMetaStoreSession;
import java.time.Clock;
import java.time.ZoneId;
import org.jetbrains.annotations.NotNull;

/**
 * The Call context is allocated each time a new REST request is processed. It contains instances of
 * low-level services required to process that request
 */
public class PolarisCallContext {

  // meta store which is used to persist Polaris entity metadata
  private final PolarisMetaStoreSession metaStore;

  // diag services
  private final PolarisDiagnostics diagServices;

  private final PolarisConfigurationStore configurationStore;

  private final Clock clock;

  public PolarisCallContext(
      @NotNull PolarisMetaStoreSession metaStore,
      @NotNull PolarisDiagnostics diagServices,
      @NotNull PolarisConfigurationStore configurationStore,
      @NotNull Clock clock) {
    this.metaStore = metaStore;
    this.diagServices = diagServices;
    this.configurationStore = configurationStore;
    this.clock = clock;
  }

  public PolarisCallContext(
      @NotNull PolarisMetaStoreSession metaStore, @NotNull PolarisDiagnostics diagServices) {
    this.metaStore = metaStore;
    this.diagServices = diagServices;
    this.configurationStore = new PolarisConfigurationStore() {};
    this.clock = Clock.system(ZoneId.systemDefault());
  }

  public PolarisMetaStoreSession getMetaStore() {
    return metaStore;
  }

  public PolarisDiagnostics getDiagServices() {
    return diagServices;
  }

  public PolarisConfigurationStore getConfigurationStore() {
    return configurationStore;
  }

  public Clock getClock() {
    return clock;
  }
}
