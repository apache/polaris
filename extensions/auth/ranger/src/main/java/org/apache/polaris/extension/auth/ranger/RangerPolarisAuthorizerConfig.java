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
package org.apache.polaris.extension.auth.ranger;

import static com.google.common.base.Preconditions.checkState;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefaults;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import io.smallrye.config.WithUnnamedKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * Configuration for Ranger authorization.
 *
 * <p>Multiple Ranger services ("tenants") can be configured. The unnamed tenant is configured
 * directly under {@code polaris.authorization.ranger.*} and is used by default. Named tenants are
 * configured under {@code polaris.authorization.ranger.<tenant-key>.*} and are selected per realm
 * via {@code polaris.authorization.authorizer.<realm>.tenant-key}. All tenants share the Ranger
 * connection {@link #properties()}.
 */
@ConfigMapping(prefix = "polaris.authorization.ranger")
public interface RangerPolarisAuthorizerConfig {

  String RANGER_KEY_PREFIX = "ranger.";
  String XASECURE_KEY_PREFIX = "xasecure.";

  String DEFAULT_TENANT_KEY = "<default>";

  /**
   * Names that cannot be used as tenant keys, as they collide with the properties of the default
   * tenant and with the Ranger connection properties.
   */
  Set<String> RESERVED_TENANT_KEYS =
      Set.of("service-name", "properties", "authz", "plugin", "xasecure");

  /** Per-tenant Ranger configuration, keyed by tenant key. */
  @WithParentName
  @WithUnnamedKey(DEFAULT_TENANT_KEY)
  @WithDefaults
  Map<String, RangerTenantConfig> tenants();

  /**
   * Ranger connection properties (e.g. {@code authz.*}, {@code xasecure.*}) forwarded to the
   * embedded Ranger authorizer. Entries take precedence over {@link #legacyProperties()}. These
   * properties are shared across all tenants.
   */
  Map<String, String> properties();

  /**
   * Legacy Ranger {@code authz.*} properties, set directly under {@code
   * polaris.authorization.ranger.authz.*}.
   *
   * @deprecated Use {@code polaris.authorization.ranger.properties.authz.*} instead.
   */
  @Deprecated(since = "1.10.0", forRemoval = true)
  @WithName("authz")
  Map<String, String> legacyAuthzProperties();

  /**
   * Legacy Ranger {@code plugin.*} properties, set directly under {@code
   * polaris.authorization.ranger.plugin.*}.
   *
   * @deprecated Use {@code polaris.authorization.ranger.properties.plugin.*} instead.
   */
  @Deprecated(since = "1.10.0", forRemoval = true)
  @WithName("plugin")
  Map<String, String> legacyPluginProperties();

  /**
   * Legacy Ranger {@code xasecure.*} properties, set directly under {@code
   * polaris.authorization.ranger.xasecure.*}.
   *
   * @deprecated Use {@code polaris.authorization.ranger.properties.xasecure.*} instead.
   */
  @Deprecated(since = "1.10.0", forRemoval = true)
  @WithName("xasecure")
  Map<String, String> legacyXasecureProperties();

  /**
   * Returns the legacy Ranger properties, i.e. those set directly under {@code
   * polaris.authorization.ranger.*} instead of {@code polaris.authorization.ranger.properties.*},
   * keyed like the entries of {@link #properties()}. These cover all the property prefixes read by
   * the embedded Ranger authorizer.
   */
  @SuppressWarnings("removal")
  default Map<String, String> legacyProperties() {
    Map<String, String> legacy = new HashMap<>();
    legacyAuthzProperties().forEach((key, value) -> legacy.put("authz." + key, value));
    legacyPluginProperties().forEach((key, value) -> legacy.put("plugin." + key, value));
    legacyXasecureProperties().forEach((key, value) -> legacy.put("xasecure." + key, value));
    return legacy;
  }

  /**
   * Converts the Ranger connection properties to a {@link Properties} object suitable for
   * initializing the Ranger embedded authorizer.
   */
  default Properties toRangerProperties() {
    Map<String, String> merged = legacyProperties();
    merged.putAll(properties());
    Properties props = new Properties();
    merged.forEach((key, value) -> {
      if (key.startsWith(XASECURE_KEY_PREFIX)) {
        props.setProperty(key, value);
      } else {
        props.setProperty(RANGER_KEY_PREFIX + key, value);
      }
    });
    return props;
  }

  default void validate() {
    checkState(
        tenants().values().stream().anyMatch(tenant -> tenant.serviceName().isPresent()),
        "At least one Ranger service name must be configured, via "
            + "polaris.authorization.ranger.service-name or "
            + "polaris.authorization.ranger.<tenant-key>.service-name");
  }

  /** Per-tenant Ranger configuration. */
  interface RangerTenantConfig {
    /** Ranger service name for this tenant. */
    Optional<String> serviceName();
  }
}
