package io.polaris.service.config;

import io.polaris.core.PolarisConfigurationStore;

/** Interface allows injection of a {@link PolarisConfigurationStore} */
public interface ConfigurationStoreAware {

  void setConfigurationStore(PolarisConfigurationStore configurationStore);
}
