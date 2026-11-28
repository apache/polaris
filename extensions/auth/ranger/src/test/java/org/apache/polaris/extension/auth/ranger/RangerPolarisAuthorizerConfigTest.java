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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class RangerPolarisAuthorizerConfigTest {

  @Test
  public void testToRangerProperties() {
    RangerPolarisAuthorizerConfig config =
        config(
            Map.of(
                "authz.default.policy.rest.url", "http://ranger:6080",
                "plugin.polaris.foo", "foo",
                "xasecure.bar", "bar"),
            Map.of(),
            Map.of(),
            Map.of());

    assertThat(config.toRangerProperties())
        .containsOnly(
            Map.entry("ranger.authz.default.policy.rest.url", "http://ranger:6080"),
            Map.entry("ranger.plugin.polaris.foo", "foo"),
            Map.entry("xasecure.bar", "bar"));
    assertThat(config.legacyProperties()).isEmpty();
  }

  @Test
  public void testToRangerPropertiesWithLegacyProperties() {
    RangerPolarisAuthorizerConfig config =
        config(
            Map.of("authz.default.policy.rest.url", "http://new:6080"),
            Map.of(
                "default.policy.rest.url", "http://legacy:6080",
                "audit.destination.solr", "enabled"),
            Map.of("polaris.foo", "foo"),
            Map.of("bar", "bar"));

    assertThat(config.legacyProperties())
        .containsOnly(
            Map.entry("authz.default.policy.rest.url", "http://legacy:6080"),
            Map.entry("authz.audit.destination.solr", "enabled"),
            Map.entry("plugin.polaris.foo", "foo"),
            Map.entry("xasecure.bar", "bar"));
    // properties() takes precedence over legacy properties
    assertThat(config.toRangerProperties())
        .containsOnly(
            Map.entry("ranger.authz.default.policy.rest.url", "http://new:6080"),
            Map.entry("ranger.authz.audit.destination.solr", "enabled"),
            Map.entry("ranger.plugin.polaris.foo", "foo"),
            Map.entry("xasecure.bar", "bar"));
  }

  static RangerPolarisAuthorizerConfig config(
      Map<String, String> properties,
      Map<String, String> legacyAuthz,
      Map<String, String> legacyPlugin,
      Map<String, String> legacyXasecure) {
    return new RangerPolarisAuthorizerConfig() {
      @Override
      public Map<String, RangerTenantConfig> tenants() {
        return Map.of(DEFAULT_TENANT_KEY, () -> Optional.of("dev_polaris"));
      }

      @Override
      public Map<String, String> properties() {
        return properties;
      }

      @Override
      @SuppressWarnings("removal")
      public Map<String, String> legacyAuthzProperties() {
        return legacyAuthz;
      }

      @Override
      @SuppressWarnings("removal")
      public Map<String, String> legacyPluginProperties() {
        return legacyPlugin;
      }

      @Override
      @SuppressWarnings("removal")
      public Map<String, String> legacyXasecureProperties() {
        return legacyXasecure;
      }
    };
  }
}
