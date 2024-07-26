package io.polaris.core;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dynamic configuration store used to retrieve runtime parameters, which may vary by realm or by
 * request.
 */
public interface PolarisConfigurationStore {

  /**
   * Retrieve the current value for a configuration key. May be null if not set.
   *
   * @param ctx the current call context
   * @param configName the name of the configuration key to check
   * @return the current value set for the configuration key or null if not set
   * @param <T> the type of the configuration value
   */
  default <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
    return null;
  }

  /**
   * Retrieve the current value for a configuration key. If not set, return the non-null default
   * value.
   *
   * @param ctx the current call context
   * @param configName the name of the configuration key to check
   * @param defaultValue the default value if the configuration key has no value
   * @return the current value or the supplied default value
   * @param <T> the type of the configuration value
   */
  default <T> @NotNull T getConfiguration(
      PolarisCallContext ctx, String configName, @NotNull T defaultValue) {
    Preconditions.checkNotNull(defaultValue, "Cannot pass null as a default value");
    T configValue = getConfiguration(ctx, configName);
    return configValue != null ? configValue : defaultValue;
  }
}
