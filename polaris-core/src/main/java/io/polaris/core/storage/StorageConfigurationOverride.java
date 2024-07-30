package io.polaris.core.storage;

import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Allows overriding the allowed locations for specific entities. Only the allowedLocations
 * specified in the constructor are allowed. allowedLocations are not inherited from the parent
 * storage configuration. All other storage configuration is inherited from the parent configuration
 * and cannot be overridden.
 */
public class StorageConfigurationOverride extends PolarisStorageConfigurationInfo {

  private final PolarisStorageConfigurationInfo parentStorageConfiguration;

  public StorageConfigurationOverride(
      @NotNull PolarisStorageConfigurationInfo parentStorageConfiguration,
      List<String> allowedLocations) {
    super(parentStorageConfiguration.getStorageType(), allowedLocations, false);
    this.parentStorageConfiguration = parentStorageConfiguration;
    allowedLocations.forEach(this::validatePrefixForStorageType);
  }

  @Override
  public String getFileIoImplClassName() {
    return parentStorageConfiguration.getFileIoImplClassName();
  }

  // delegate to the wrapped class in case they override the parent behavior
  @Override
  protected void validatePrefixForStorageType(String loc) {
    parentStorageConfiguration.validatePrefixForStorageType(loc);
  }

  @Override
  public void validateMaxAllowedLocations(int maxAllowedLocations) {
    parentStorageConfiguration.validateMaxAllowedLocations(maxAllowedLocations);
  }
}
