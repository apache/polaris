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

package org.apache.polaris.core.config;

import static org.apache.polaris.core.config.RealmConfigurationSource.EMPTY_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.polaris.core.entity.CatalogEntity;
import org.junit.jupiter.api.Test;

class RealmConfigImplTest {

  private static final RealmConfigurationSource stringsSource =
      (rc, name) -> String.format("value-%s-%s", rc.getRealmIdentifier(), name);

  private RealmConfig strings(String realmName) {
    return new RealmConfigImpl(stringsSource, () -> realmName);
  }

  private RealmConfig empty() {
    return new RealmConfigImpl(EMPTY_CONFIG, () -> "test-realm");
  }

  @Test
  @SuppressWarnings("removal")
  void legacyLookup() {
    assertThat((Object) strings("rc1").getConfig("c1")).isEqualTo("value-rc1-c1");
    assertThat((Object) strings("rc2").getConfig("c1", "default1")).isEqualTo("value-rc2-c1");
    assertThat((Object) empty().getConfig("c1", "default1")).isEqualTo("default1");
  }

  @Test
  void entityProperties() {
    FeatureConfiguration<String> cfg =
        PolarisConfiguration.<String>builder()
            .key("TEST-ENTITY1")
            .catalogConfig("polaris.config.test-entity-prop")
            .description("test")
            .defaultValue("default2")
            .buildFeatureConfiguration();

    CatalogEntity entity =
        new CatalogEntity.Builder()
            .setProperties(Map.of("polaris.config.test-entity-prop", "entity2"))
            .build();

    assertThat(strings("rc1").getConfig(cfg)).isEqualTo("value-rc1-TEST-ENTITY1");
    assertThat(strings("rc2").getConfig(cfg, entity)).isEqualTo("entity2");
  }
}
