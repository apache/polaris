package io.polaris.core.storage;

import org.jetbrains.annotations.Nullable;

/**
 * Factory interface that knows how to construct a {@link PolarisStorageIntegration} given a {@link
 * PolarisStorageConfigurationInfo}.
 */
public interface PolarisStorageIntegrationProvider {
  @SuppressWarnings("unchecked")
  <T extends PolarisStorageConfigurationInfo> @Nullable
      PolarisStorageIntegration<T> getStorageIntegrationForConfig(
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo);
}
