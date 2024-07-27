package io.polaris.service.config;

import io.polaris.core.PolarisCallContext;
import io.polaris.core.PolarisConfigurationStore;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

public class DefaultConfigurationStore implements PolarisConfigurationStore {
  private final Map<String, Object> properties;

  public DefaultConfigurationStore(Map<String, Object> properties) {
    this.properties = properties;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
    return (T) properties.get(configName);
  }
}
